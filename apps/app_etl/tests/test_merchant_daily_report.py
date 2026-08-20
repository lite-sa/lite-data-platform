"""Unit tests for the merchant daily report export — pure transforms only,
no BigQuery/GCS involved (CI never gets credentials; same stance as the
notify-job test).
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from app_etl.export.merchant_daily_report import (
    _empty_settlement,
    build_report,
    dedupe_settlement,
    pivot_fees,
    run_checks,
    write_daily_files,
)

M1, M2 = "merchant-1", "merchant-2"


def _payments() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": ["p1", "p2"],
            "merchant_id": [M1, M1],
            "merchant_name": ["Merchant One", "Merchant One"],
            "amount": [1243, 5800],
            "currency": ["SAR", "SAR"],
            "status": ["CAPTURED", "CAPTURED"],
            "created_at": pd.to_datetime(
                ["2026-08-19 13:49:16+00:00", "2026-08-19 18:01:44+00:00"],
                utc=True,
            ),
            "order_data": ['{"reference":"ord-1"}', None],
            "metadata": [None, None],  # all-NaN -> dropped from M1's file
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

    bad_tie = settle.assign(window_status="SUCCESS", settled_amount=999)
    assert any("amount - fees" in f for f in run_checks(_payments(), bad_tie))
    # The same mismatch while the window is in flight is not blocking.
    inflight = settle.assign(settled_amount=999)
    assert run_checks(_payments(), inflight) == []


def test_build_report_grain_and_conversions():
    report = build_report(_payments(), _prepared_settle())
    assert len(report) == 2  # payment grain survives the join

    p1 = report[report["id"] == "p1"].iloc[0]
    assert p1["payment_creation_date"] == date(2026, 8, 19)
    assert p1["amount_sar"] == pytest.approx(12.43)
    assert p1["fee_total_sar"] == pytest.approx(0.31)
    assert p1["settled_amount_sar"] == pytest.approx(12.12)
    assert p1["order_reference"] == "ord-1"
    assert p1["window_status"] == "MATURED"

    p2 = report[report["id"] == "p2"].iloc[0]
    assert pd.isna(p2["settlement_transaction_id"])
    assert pd.isna(p2["fee_total_sar"])  # no settlement != zero fee


def test_write_daily_files_one_per_merchant_including_empty(tmp_path):
    report = build_report(_payments(), _prepared_settle())
    files = write_daily_files(
        report, date(2026, 8, 19), date(2026, 8, 25), tmp_path, merchant_ids=[M1, M2]
    )
    assert [(mid, n) for mid, _, n in files] == [(M1, 2), (M2, 0)]
    # Filename pairs report day with run date — backfills stay traceable.
    assert files[0][1].name == "transactions_2026-08-19_run_2026-08-25.csv"

    m1 = pd.read_csv(files[0][1])
    assert len(m1) == 2
    assert "metadata" not in m1.columns  # all-NaN dropped for the merchant

    m2 = pd.read_csv(files[1][1])  # header-only file, full schema kept
    assert len(m2) == 0
    assert "settlement_transaction_id" in m2.columns
    assert "fee_total_sar" in m2.columns
