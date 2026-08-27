"""Unit tests for the merchant daily report export — pure transforms only,
no BigQuery/GCS involved (CI never gets credentials; same stance as the
notify-job test).
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from app_etl.export.merchant_daily_report import (
    EXCLUDED_PAYMENT_IDS,
    EXPORT_COLUMNS,
    _empty_operations,
    _empty_settlement,
    build_report,
    dedupe_settlement,
    merchant_slug,
    pivot_fees,
    pos_identifiers,
    run_checks,
    write_daily_files,
)

M1, M2 = "merchant-1", "merchant-2"
MERCHANTS = {M1: "Merchant One", M2: "Merchant Two"}


def _payments() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": ["p1", "p2"],
            "merchant_id": [M1, M1],
            "merchant_name": ["Merchant One", "Merchant One"],
            "amount": [1243, 5800],
            "currency": ["SAR", "SAR"],
            "status": ["CAPTURED", "CAPTURED"],
            "capture_mode": ["INSTANT", "INSTANT"],
            "created_at": pd.to_datetime(
                ["2026-08-19 13:49:16+00:00", "2026-08-19 18:01:44+00:00"],
                utc=True,
            ),
            "order_data": ['{"reference":"ord-1"}', None],
            "instrument_data": ['{"last_four":"1172","card_brand":"MADA"}', None],
            "channel_id": ["ch-1", "ch-1"],
            "channel_name": ["SKEWRD", "SKEWRD"],
            "channel_merchant_id": [M1, M1],
            "channel_type": ["IN-PERSON", "IN-PERSON"],
            "metadata": [None, None],  # not in the keep-list, never ships
        }
    )


def _ops() -> pd.DataFrame:
    # p1: init row (random UUID key), then the sale receipt, then a refund
    # receipt carrying its own rrn — the earliest ';' key (the sale) must
    # win. p2: device-error path, no receipt key at all.
    return pd.DataFrame(
        {
            "payment_id": ["p1", "p1", "p1", "p2"],
            "idempotency_key": [
                "0b0e6c9a-6a54-4e0e-9c8e-2f7f6f3e2d10",
                "623600000156;000156;8193375101300001",
                "623700000201;000201;8193375101300001",
                None,
            ],
            "created_at": pd.to_datetime(
                [
                    "2026-08-19 13:49:16+00:00",
                    "2026-08-19 13:49:20+00:00",
                    "2026-08-21 09:00:00+00:00",
                    "2026-08-19 18:01:44+00:00",
                ],
                utc=True,
            ),
        }
    )


def _settle() -> pd.DataFrame:
    # amount - fees == settled holds: 1243 - 31 == 1212.
    return pd.DataFrame(
        {
            "id": ["t1"],
            "parent_payment_id": ["p1"],
            "settlement_window_id": ["w1"],
            "status": ["ACTIVE"],
            "is_settled": [False],
            "amount": [1243],
            "settled_amount": [1212],
            "fees": [
                '[{"fee_type":"mdr","amount":27},{"fee_type":"vat","amount":4}]'
            ],
            "created_at": pd.to_datetime(["2026-08-19 14:00:00+00:00"], utc=True),
            "window_merchant_id": [M1],
            "window_status": ["MATURED"],
            "value_day": ["2026-08-20"],
        }
    )


def _prepared_settle() -> pd.DataFrame:
    settle, _ = pivot_fees(_settle())
    settle, _ = dedupe_settlement(settle)
    return settle


def _report() -> pd.DataFrame:
    return build_report(_payments(), _prepared_settle(), pos_identifiers(_ops()))


def test_pivot_fees_pivots_and_keeps_base_columns():
    settle, fee_cols = pivot_fees(_settle())
    row = settle.iloc[0]
    assert row["fee_mdr_minor"] == 27
    assert row["fee_vat_minor"] == 4
    assert row["fee_flat_minor"] == 0  # base type absent from the JSON -> 0
    assert row["fee_total_minor"] == 31
    assert fee_cols[:3] == ["fee_mdr_minor", "fee_vat_minor", "fee_flat_minor"]


def test_pivot_fees_empty_frame_keeps_schema():
    settle, fee_cols = pivot_fees(_empty_settlement())
    assert set(fee_cols) <= set(settle.columns)
    assert "fee_total_minor" in settle.columns


def test_pos_identifiers_parses_earliest_sale_key():
    ids = pos_identifiers(_ops())
    assert list(ids["payment_id"]) == ["p1"]  # p2 has no sale receipt
    row = ids.iloc[0]
    assert row["RRN"] == "623600000156"  # the sale, not the later refund
    assert row["STAN"] == "000156"  # leading zeros survive (string, not int)
    assert row["TID"] == "8193375101300001"


def test_pos_identifiers_empty_ops_keeps_schema():
    ids = pos_identifiers(_empty_operations())
    assert list(ids.columns) == ["payment_id", "RRN", "STAN", "TID"]
    assert len(ids) == 0


def test_dedupe_keeps_earliest_sale_and_warns():
    refund = _settle().assign(
        id="t2",
        created_at=pd.Timestamp("2026-08-21 09:00:00+00:00"),
        amount=-1243,
        settled_amount=-1243,
        fees=None,
    )
    settle, _ = pivot_fees(pd.concat([_settle(), refund], ignore_index=True))
    deduped, warnings = dedupe_settlement(settle)
    assert len(deduped) == 1
    assert deduped.iloc[0]["id"] == "t1"  # the sale, not the refund
    assert deduped.iloc[0]["n_settlement_txns"] == 2
    assert len(warnings) == 1 and "p1" in warnings[0]


def test_checks_block_cross_tenant_and_terminal_tieout():
    settle = _prepared_settle()
    assert run_checks(_payments(), settle) == []

    cross = settle.assign(window_merchant_id=M2)
    assert any("window merchant" in f for f in run_checks(_payments(), cross))

    cross_chan = _payments().assign(channel_merchant_id=M2)
    assert any("channel belongs" in f for f in run_checks(cross_chan, settle))
    # An unresolvable channel (NaN owner) is not a mismatch.
    no_chan = _payments().assign(channel_merchant_id=None)
    assert run_checks(no_chan, settle) == []

    bad_tie = settle.assign(window_status="SUCCESS", settled_amount=999)
    assert any("amount - fees" in f for f in run_checks(_payments(), bad_tie))
    # The same mismatch while the window is in flight is not blocking.
    inflight = settle.assign(settled_amount=999)
    assert run_checks(_payments(), inflight) == []


def test_build_report_grain_and_conversions():
    report = _report()
    assert len(report) == 2  # payment grain survives the joins

    p1 = report[report["id"] == "p1"].iloc[0]
    assert p1["payment_creation_date"] == date(2026, 8, 19)
    assert p1["amount_sar"] == pytest.approx(12.43)
    assert p1["fee_total_sar"] == pytest.approx(0.31)
    assert p1["settled_amount_sar"] == pytest.approx(12.12)
    assert p1["order_reference"] == "ord-1"
    assert p1["window_status"] == "MATURED"
    assert p1["last_four"] == "1172"
    assert p1["card_brand"] == "MADA"
    assert p1["RRN"] == "623600000156"
    assert p1["STAN"] == "000156"
    assert p1["TID"] == "8193375101300001"

    p2 = report[report["id"] == "p2"].iloc[0]
    assert pd.isna(p2["settlement_transaction_id"])
    assert pd.isna(p2["fee_total_sar"])  # no settlement != zero fee
    assert pd.isna(p2["last_four"])  # no instrument_data
    assert pd.isna(p2["RRN"])  # no sale receipt


def test_write_daily_files_one_per_merchant_including_empty(tmp_path):
    files = write_daily_files(
        _report(), date(2026, 8, 19), date(2026, 8, 25), tmp_path, merchants=MERCHANTS
    )
    assert [(mid, n) for mid, _, n in files] == [(M1, 2), (M2, 0)]
    # Filename: name slug + report day + run date — backfills stay traceable.
    assert files[0][1].name == (
        "merchant_one_daily_transaction_report_2026-08-19_run_2026-08-25.csv"
    )

    m1 = pd.read_csv(files[0][1])
    assert len(m1) == 2
    assert "metadata" not in m1.columns  # outside the keep-list, never ships
    # The header is fixed: the full keep-list, in order, every file.
    assert list(m1.columns) == EXPORT_COLUMNS

    m2 = pd.read_csv(files[1][1])  # header-only file, same fixed schema
    assert len(m2) == 0
    assert list(m2.columns) == EXPORT_COLUMNS


def test_fixed_header_keeps_all_nan_columns(tmp_path):
    # A merchant-day where no payment has a sale receipt (all-ecom, say)
    # still ships RRN/STAN/TID — empty, never missing (2026-08-27 ask).
    report = build_report(
        _payments(), _prepared_settle(), pos_identifiers(_empty_operations())
    )
    files = write_daily_files(
        report, date(2026, 8, 19), date(2026, 8, 25), tmp_path, merchants={M1: "M One"}
    )
    m1 = pd.read_csv(files[0][1])
    assert len(m1) == 2
    assert list(m1.columns) == EXPORT_COLUMNS
    assert m1["RRN"].isna().all()


def test_export_keep_list_matches_the_ops_sample(tmp_path):
    # Fixed layout (ops sample sign-off 2026-08-27): raw-unit / blob /
    # internal columns never ship, even when populated — a keep-list, not
    # a drop-list.
    files = write_daily_files(
        _report(), date(2026, 8, 19), date(2026, 8, 25), tmp_path, merchants={M1: "M One"}
    )
    m1 = pd.read_csv(files[0][1])
    for dropped in [
        "amount",
        "order_data",
        "created_at",
        "updated_at",
        "risk_result",
        "instrument_data",
        "metadata",
        "payment_method",
        "channel_merchant_id",
        "n_settlement_txns",
        "fee_mdr_minor",
        "fee_total_minor",
        "settled_amount_minor",
    ]:
        assert dropped not in m1.columns
        assert dropped not in EXPORT_COLUMNS
    for kept in [
        "amount_sar",
        "payment_creation_date",  # back in, per the sample
        "created_at_local",
        "order_reference",
        "RRN",
        "STAN",
        "TID",
        "channel_name",
        "last_four",
        "card_brand",
        "settled_amount_sar",
    ]:
        assert kept in m1.columns
        assert kept in EXPORT_COLUMNS


def test_merchant_slug():
    assert merchant_slug("Alatima Alraeia Company Ltd.", "x") == (
        "alatima_alraeia_company_ltd"
    )
    assert merchant_slug("J hub", "x") == "j_hub"
    # Nothing slug-safe survives -> id prefix, never an empty filename.
    assert merchant_slug("شركة", "abcdef1234") == "abcdef12"


def test_excluded_payment_ids_match_dbt_seed():
    # The canonical exclusion list is the dbt seed; the export carries a
    # copy because its query runs in parallel with dbt build and cannot
    # depend on the seed table existing. This is the lockstep guard.
    seed = pd.read_csv(
        Path(__file__).parents[1] / "dbt" / "seeds" / "incident_excluded_payments.csv"
    )
    assert sorted(seed["payment_id"]) == sorted(EXCLUDED_PAYMENT_IDS)
