"""SAMA queries against prod BigQuery for one report month.

CI has no BQ credentials, so these run only on demand:
SAMA_BQ_TESTS=1 uv run pytest apps/app_etl/tests/test_sama_reports_bq.py
"""

import importlib
import os
from pathlib import Path

import pandas as pd
import pytest

from app_etl.export.sama.common import (
    DELETED_MERCHANT_IDS,
    RAW,
    SCHEME_CORRECTIONS,
    TEST_MERCHANT_IDS,
    run,
)

pytestmark = pytest.mark.skipif(
    os.environ.get("SAMA_BQ_TESTS") != "1", reason="needs prod BigQuery"
)

MONTH = "2026-09"
SEEDS = Path(__file__).parents[1] / "dbt" / "seeds"


@pytest.fixture(scope="module")
def details() -> dict[str, pd.DataFrame]:
    from app_etl.export.sama.__main__ import data_points

    return {
        name: run(importlib.import_module(name).DETAIL_SQL, MONTH)
        for name in data_points()
    }


@pytest.fixture(scope="module")
def authorized() -> pd.DataFrame:
    from app_etl.export.sama.pi.common import AUTHORIZED_PAYMENTS

    return run(f"select * from {AUTHORIZED_PAYMENTS}", MONTH)


def answer(name: str) -> pd.DataFrame:
    return run(importlib.import_module(f"app_etl.export.sama.{name}").ANSWER_SQL, MONTH)


# Scheme corrections


def test_every_scheme_correction_is_authorized_in_the_month(authorized):
    corrected = authorized[authorized["payment_id"].isin(SCHEME_CORRECTIONS)]
    assert sorted(corrected["payment_id"]) == sorted(SCHEME_CORRECTIONS)


def test_scheme_corrections_win_over_card_brand(authorized):
    corrected = authorized[authorized["payment_id"].isin(SCHEME_CORRECTIONS)]
    expected = corrected["payment_id"].map(SCHEME_CORRECTIONS)
    assert (corrected["scheme"] == expected).all()
    assert (corrected["scheme_reported"] == expected).all()
    assert (corrected["scheme_source"] == "correction").all()
    assert (authorized["scheme_source"] == "correction").sum() == len(corrected)


def test_scheme_corrections_still_fix_a_wrong_card_brand():
    # Fails once upstream fixes card_brand: the seed row is then redundant.
    seed = pd.read_csv(SEEDS / "scheme_corrections.csv")
    ids = ", ".join(f"'{i}'" for i in seed["payment_id"])
    raw = run(
        f"""
        select id as payment_id,
               upper(json_value(instrument_data, '$.card_brand')) as card_brand
        from `{RAW}.payment_v2__payments`
        where id in ({ids})
        qualify row_number() over (
            partition by id order by updated_at desc, _dlt_load_id desc
        ) = 1
        """,
        MONTH,
    )
    recorded = dict(zip(seed["payment_id"], seed["recorded_scheme"]))
    assert dict(zip(raw["payment_id"], raw["card_brand"])) == recorded


def test_scheme_grid_counts_the_corrected_scheme(authorized):
    grid = answer("pi.s3_01_transactions_by_scheme").set_index(["channel", "scheme"])
    for (channel, scheme), row in grid.iterrows():
        rows = authorized
        if channel != "All":
            rows = rows[rows["channel"] == channel]
        if scheme != "Total":
            rows = rows[rows["scheme_reported"] == scheme]
        assert row["transactions"] == len(rows), (channel, scheme)
        assert row["volume_sar"] == pytest.approx(rows["amount_sar"].sum()), (
            channel,
            scheme,
        )


# Hard-deleted merchants


def test_raw_still_holds_a_deleted_merchant():
    # Guards the tests below: with no raw rows they would pass vacuously.
    rows = run(
        f"select count(*) as n from `{RAW}.ledger__account` "
        "where tag = 'WALLET' and owner in unnest(@deleted_ids)",
        MONTH,
    )
    assert rows["n"].iloc[0] > 0


def test_no_detail_row_names_a_deleted_merchant(details):
    for name, detail in details.items():
        for col in (c for c in detail.columns if c.endswith("merchant_id")):
            assert not detail[col].isin(DELETED_MERCHANT_IDS).any(), (name, col)


def test_answers_count_no_deleted_wallet(details):
    wallets = details["app_etl.export.sama.emi.s1_01_outstanding_balance"]
    assert answer("emi.s1_01_outstanding_balance")["wallets"].iloc[0] == len(wallets)
    assert not wallets["merchant_id"].isin(DELETED_MERCHANT_IDS).any()


# Test merchants stay in scope


def test_test_merchants_reach_the_detail_rows(details, authorized):
    assert authorized["is_test"].any()
    assert (
        authorized.loc[authorized["is_test"], "merchant_id"]
        .isin(TEST_MERCHANT_IDS)
        .all()
    )
    kyb = details["app_etl.export.sama.pi.s1_01_ecom_merchants"]
    assert set(TEST_MERCHANT_IDS) <= set(kyb.loc[kyb["in_population"], "merchant_id"])
    wallets = details["app_etl.export.sama.emi.s1_01_outstanding_balance"]
    assert wallets["is_test"].any()


def test_answers_count_test_merchants(details, authorized):
    kyb = details["app_etl.export.sama.pi.s1_01_ecom_merchants"]
    assert answer("pi.s1_01_ecom_merchants")["ecom_merchants"].iloc[0] == (
        kyb["in_population"].sum()
    )
    grid = answer("pi.s3_01_transactions_by_scheme").set_index(["channel", "scheme"])
    assert grid.loc[("All", "Total"), "transactions"] == len(authorized)
    wallets = details["app_etl.export.sama.emi.s1_01_outstanding_balance"]
    balance = answer("emi.s1_01_outstanding_balance")
    assert balance["outstanding_balance_sar"].iloc[0] == pytest.approx(
        wallets["balance_sar"].sum()
    )
