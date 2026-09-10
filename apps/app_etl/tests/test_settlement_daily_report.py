"""Unit tests for the settlement daily report export (v2) — pure transforms
only, no BigQuery/GCS involved (CI never gets credentials; same stance as
the merchant-report and notify-job tests).

Coverage: one poisoned fixture per gate check in run_checks, both gate
waivers (they fire only for their own id, and never in silence), the
NULL-parent_payment_id fallback through the operation, the
informational notes, the refund row's original-payment semantics in
build_report, the leftover payment-grain path, the file writers, and the
seed-lockstep guard for the incident exclusion list.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from app_etl.export.settlement_daily_report import (
    EXCLUDED_PAYMENT_IDS,
    EXPORT_COLUMNS,
    _empty_operations,
    _empty_settlement,
    build_payment_rows,
    build_report,
    dedupe_settlement,
    export_columns,
    informational_notes,
    merchant_slug,
    pivot_fees,
    pivot_fees_txn,
    pos_identifiers,
    WAIVED_TIE_OUT_WINDOWS,
    WAIVED_UNRESOLVED_TXNS,
    run_checks,
    run_payment_checks,
    write_combined_file,
    write_daily_files,
)

M1, M2 = "merchant-1", "merchant-2"
MERCHANTS = {M1: "Merchant One", M2: "Merchant Two"}

P1_CREATED = "2026-08-19 13:49:16+00:00"  # 16:49 Riyadh, same calendar day


def _spine() -> pd.DataFrame:
    # Four transactions across two windows, all merchant M1:
    #   t1  sale of p1 in w1 (fees mdr 27 + vat 4, 1243 - 31 == 1212)
    #   t2  refund of p1 in w1 (negative, born HELD, no fees)
    #   t3  REMOVED voided sale of p3 in w2 — its broken tie-out
    #       (500 - 0 != 480) and NULL operation must never block
    #   t4  sale of an incident-excluded payment in w2 — in the spine for
    #       the window tie-out, dropped from the file by build_report
    # Window sums over included rows: w1 = 1212 - 1243 = -31, w2 = 700.
    return pd.DataFrame(
        {
            "settlement_transaction_id": ["t1", "t2", "t3", "t4"],
            "payment_id": ["p1", "p1", "p3", EXCLUDED_PAYMENT_IDS[0]],
            "operation_id": ["op-1", "op-2", "op-3", "op-4"],
            "operation": ["pay", "refund", None, "pay"],
            "settlement_status": ["SETTLED", "HELD", "REMOVED", "SETTLED"],
            "included_in_window": [True, True, False, True],
            "is_settled": [True, False, False, True],
            "amount_minor": [1243, -1243, 500, 700],
            "settled_amount_minor": [1212, -1243, 480, 700],
            "fees": [
                '[{"fee_type":"mdr","amount":27},{"fee_type":"vat","amount":4}]',
                None,
                None,
                None,
            ],
            "currency": ["SAR", "SAR", "SAR", "SAR"],
            "hold_at": pd.to_datetime(
                [None, "2026-08-25 09:59:00+00:00", None, None], utc=True
            ),
            "txn_created_at": pd.to_datetime(
                [
                    "2026-08-19 14:00:00+00:00",
                    "2026-08-25 10:00:00+00:00",
                    "2026-08-19 15:00:00+00:00",
                    "2026-08-19 16:00:00+00:00",
                ],
                utc=True,
            ),
            "settlement_window_id": ["w1", "w1", "w2", "w2"],
            "merchant_id": [M1, M1, M1, M1],
            "window_status": ["MATURED", "MATURED", "SUCCESS", "SUCCESS"],
            "window_settled_amount_minor": [-31, -31, 700, 700],
            "window_currency": ["SAR", "SAR", "SAR", "SAR"],
            "value_day": ["2026-08-26"] * 4,
            "moved_across_days": [False, False, False, True],
            "merchant_name": ["Merchant One"] * 4,
            "op_resolved": [True, True, True, True],
            "payment_resolved": [True, True, True, True],
            "payment_resolved_via_op": [False, False, False, False],
            "payment_merchant_id": [M1, M1, M1, M1],
            "payment_status": ["REFUNDED", "REFUNDED", "FAILED", "CAPTURED"],
            "payment_currency": ["SAR", "SAR", "SAR", "SAR"],
            "capture_mode": ["INSTANT"] * 4,
            "payment_instrument_id": ["pi-1", "pi-1", None, None],
            "instrument_data": [
                '{"last_four":"1172","card_brand":"MADA"}',
                '{"last_four":"1172","card_brand":"MADA"}',
                None,
                None,
            ],
            "channel_id": ["ch-1", "ch-1", None, "ch-1"],
            "channel_type": ["IN-PERSON"] * 4,
            "order_data": ['{"reference":"ord-1"}', '{"reference":"ord-1"}', None, None],
            "created_at": pd.to_datetime(
                [P1_CREATED, P1_CREATED, P1_CREATED, P1_CREATED], utc=True
            ),
            "channel_name": ["SKEWRD", "SKEWRD", None, "SKEWRD"],
            "channel_merchant_id": [M1, M1, None, M1],
        }
    )


def _prepared_spine() -> tuple[pd.DataFrame, list[str]]:
    return pivot_fees_txn(_spine())


def _day_ops() -> pd.DataFrame:
    # A booked refund (window path) and a REVERSE (note only). A refund
    # with has_settlement_row=False is the wallet path — note, never a
    # gate failure (see the wallet-note test).
    return pd.DataFrame(
        {
            "operation_type": ["REFUND", "REVERSE"],
            "op_id": ["op-r1", "op-v1"],
            "payment_id": ["p1", "p9"],
            "has_settlement_row": [True, False],
        }
    )


def _ops() -> pd.DataFrame:
    # p1: init row (random UUID key), then the sale receipt, then a refund
    # receipt with its own rrn — the earliest ';' key (the sale) must win.
    return pd.DataFrame(
        {
            "payment_id": ["p1", "p1", "p1"],
            "idempotency_key": [
                "0b0e6c9a-6a54-4e0e-9c8e-2f7f6f3e2d10",
                "623600000156;000156;8193375101300001",
                "623700000201;000201;8193375101300001",
            ],
            "created_at": pd.to_datetime(
                [
                    "2026-08-19 13:49:16+00:00",
                    "2026-08-19 13:49:20+00:00",
                    "2026-08-25 10:00:00+00:00",
                ],
                utc=True,
            ),
        }
    )


def _leftover_payments() -> pd.DataFrame:
    # A payment the settlement spine cannot see (device error, no txn).
    return pd.DataFrame(
        {
            "id": ["p2"],
            "merchant_id": [M1],
            "merchant_name": ["Merchant One"],
            "amount": [5800],
            "currency": ["SAR"],
            "status": ["FAILED"],
            "capture_mode": ["INSTANT"],
            "created_at": pd.to_datetime(["2026-08-19 18:01:44+00:00"], utc=True),
            "order_data": [None],
            "instrument_data": [None],
            "channel_id": ["ch-1"],
            "channel_name": ["SKEWRD"],
            "channel_merchant_id": [M1],
            "channel_type": ["IN-PERSON"],
        }
    )


def _prepared_empty_settle() -> pd.DataFrame:
    # Exactly main()'s leftover-path stand-in.
    settle, _ = pivot_fees(_empty_settlement())
    settle, _ = dedupe_settlement(settle)
    return settle


# --- shared-list lockstep guards -------------------------------------------


def test_excluded_payment_ids_match_dbt_seed():
    # The canonical exclusion list is the dbt seed; the export carries a
    # copy because its query runs in parallel with dbt build and cannot
    # depend on the seed table existing. This is the lockstep guard.
    seed = pd.read_csv(
        Path(__file__).parents[1] / "dbt" / "seeds" / "incident_excluded_payments.csv"
    )
    assert sorted(seed["payment_id"]) == sorted(EXCLUDED_PAYMENT_IDS)


# --- transforms ------------------------------------------------------------


def test_pivot_fees_txn_keys_on_transaction_id():
    spine, fee_cols = _prepared_spine()
    assert "settlement_transaction_id" in spine.columns
    assert "id" not in spine.columns  # the rename shim must leave no trace
    t1 = spine[spine["settlement_transaction_id"] == "t1"].iloc[0]
    assert t1["fee_mdr_minor"] == 27
    assert t1["fee_vat_minor"] == 4
    assert t1["fee_flat_minor"] == 0  # base type absent from the JSON -> 0
    assert t1["fee_total_minor"] == 31
    t2 = spine[spine["settlement_transaction_id"] == "t2"].iloc[0]
    assert t2["fee_total_minor"] == 0  # fees NULL on the refund row
    assert set(fee_cols) == {"fee_mdr_minor", "fee_vat_minor", "fee_flat_minor"}


def test_pos_identifiers_parses_earliest_sale_key():
    ids = pos_identifiers(_ops())
    row = ids.iloc[0]
    assert row["RRN"] == "623600000156"  # the sale, not the later refund
    assert row["STAN"] == "000156"  # leading zeros survive (string, not int)
    assert row["TID"] == "8193375101300001"


def test_pos_identifiers_empty_ops_keeps_schema():
    ids = pos_identifiers(_empty_operations())
    assert list(ids.columns) == ["payment_id", "RRN", "STAN", "TID"]
    assert len(ids) == 0


# --- the gate --------------------------------------------------------------


def test_checks_pass_on_good_spine():
    # Note what "good" already contains: a REMOVED row with a broken
    # tie-out and a NULL operation — both must stay invisible to the gate.
    spine, _ = _prepared_spine()
    assert run_checks(spine) == []


def test_checks_block_unresolved_joins_and_duplicates():
    spine, _ = _prepared_spine()

    dup = spine.assign(settlement_transaction_id="t1")
    assert any("duplicate settlement" in f for f in run_checks(dup))

    no_op = spine.copy()
    no_op.loc[no_op["settlement_transaction_id"] == "t1", "op_resolved"] = False
    assert any("no payment_operation" in f for f in run_checks(no_op))

    no_pay = spine.copy()
    no_pay.loc[
        no_pay["settlement_transaction_id"] == "t1", "payment_resolved"
    ] = False
    assert any(
        "no payment for parent_payment_id" in f for f in run_checks(no_pay)
    )


def test_checks_block_cross_tenant_and_foreign_currency():
    spine, _ = _prepared_spine()

    cross = spine.copy()
    cross.loc[cross["settlement_transaction_id"] == "t1", "payment_merchant_id"] = M2
    assert any("window merchant" in f for f in run_checks(cross))

    cross_chan = spine.copy()
    cross_chan.loc[
        cross_chan["settlement_transaction_id"] == "t1", "channel_merchant_id"
    ] = M2
    assert any("channel belongs" in f for f in run_checks(cross_chan))

    foreign = spine.copy()
    foreign.loc[foreign["settlement_transaction_id"] == "t1", "payment_currency"] = (
        "USD"
    )
    assert any("non-SAR" in f for f in run_checks(foreign))


def test_checks_block_amount_and_sign_violations():
    spine, _ = _prepared_spine()

    bad_tie = spine.copy()
    bad_tie.loc[
        bad_tie["settlement_transaction_id"] == "t1", "settled_amount_minor"
    ] = 999
    assert any("amount - fees" in f for f in run_checks(bad_tie))

    # Every negative row must be born HELD.
    no_hold = spine.assign(hold_at=pd.NaT)
    assert any("without hold_at" in f for f in run_checks(no_hold))

    # operation says refund, sign says sale (and vice versa).
    op_sign = spine.copy()
    op_sign.loc[op_sign["settlement_transaction_id"] == "t2", "operation"] = "pay"
    assert any("sign disagree" in f for f in run_checks(op_sign))


def test_checks_block_window_tieout():
    spine, _ = _prepared_spine()

    bad_win = spine.copy()
    bad_win.loc[
        bad_win["settlement_window_id"] == "w1", "window_settled_amount_minor"
    ] = 0
    failures = run_checks(bad_win)
    assert any("1 window(s)" in f and "w1" in f for f in failures)


def test_waived_adjustment_entry_passes_only_for_its_own_id():
    # The manual VAT-remediation entry resolves neither reference. Its id
    # is waived; the same breakage on any other row still blocks.
    spine, _ = _prepared_spine()
    waived_id = next(iter(WAIVED_UNRESOLVED_TXNS))

    adjustment = spine.copy()
    adjustment.loc[
        adjustment["settlement_transaction_id"] == "t1",
        ["settlement_transaction_id", "op_resolved", "payment_resolved"],
    ] = [waived_id, False, False]
    assert run_checks(adjustment) == []

    # Same two broken joins, an id nobody investigated: still blocked.
    stranger = spine.copy()
    stranger.loc[
        stranger["settlement_transaction_id"] == "t1",
        ["op_resolved", "payment_resolved"],
    ] = False
    failures = run_checks(stranger)
    assert any("no payment_operation" in f for f in failures)
    assert any("no payment for parent_payment_id" in f for f in failures)

    # A waived row does not cover an unresolved row sitting beside it.
    both = adjustment.copy()
    both.loc[both["settlement_transaction_id"] == "t2", "op_resolved"] = False
    assert any("1 rows with no payment_operation" in f for f in run_checks(both))


def test_null_parent_resolved_via_operation_passes_and_is_noted():
    # A settlement row whose parent_payment_id is NULL still resolves its
    # payment through the operation (first case 2026-09-09). The gate
    # stays quiet, and the notes name the row so the upstream loss of
    # the payment id never goes unreported.
    spine, _ = _prepared_spine()
    assert not any(
        "NULL parent_payment_id" in n for n in informational_notes(spine, _day_ops())
    )

    via_op = spine.copy()
    via_op.loc[
        via_op["settlement_transaction_id"] == "t1", "payment_resolved_via_op"
    ] = True
    assert run_checks(via_op) == []
    notes = "\n".join(informational_notes(via_op, _day_ops()))
    assert (
        "1 row(s) with NULL parent_payment_id resolved through the "
        "operation's payment_id" in notes
    )
    assert "['t1']" in notes


def test_waived_window_tieout_passes_only_for_its_own_window():
    spine, _ = _prepared_spine()
    waived_window = next(iter(WAIVED_TIE_OUT_WINDOWS))

    broken = spine.copy()
    broken.loc[
        broken["settlement_window_id"] == "w1", "window_settled_amount_minor"
    ] = 0
    assert any("1 window(s)" in f and "w1" in f for f in run_checks(broken))

    waived = broken.copy()
    waived.loc[waived["settlement_window_id"] == "w1", "settlement_window_id"] = (
        waived_window
    )
    assert run_checks(waived) == []


def test_waivers_are_never_silent():
    # A waived row that ships must say so in the notes, or nobody learns
    # the gate stayed quiet on purpose.
    spine, _ = _prepared_spine()
    waived_id = next(iter(WAIVED_UNRESOLVED_TXNS))
    waived_window = next(iter(WAIVED_TIE_OUT_WINDOWS))
    assert not any("WAIVED" in n for n in informational_notes(spine, _day_ops()))

    carrying = spine.copy()
    carrying.loc[
        carrying["settlement_transaction_id"] == "t1", "settlement_transaction_id"
    ] = waived_id
    carrying.loc[
        carrying["settlement_window_id"] == "w1", "settlement_window_id"
    ] = waived_window
    notes = "\n".join(informational_notes(carrying, _day_ops()))
    assert f"WAIVED join contract for ['{waived_id}']" in notes
    assert f"WAIVED per-window tie-out for ['{waived_window}']" in notes


def test_payment_checks_on_leftover_path():
    settle = _prepared_empty_settle()
    assert run_payment_checks(_leftover_payments(), settle) == []

    dup = pd.concat([_leftover_payments()] * 2, ignore_index=True)
    assert any("duplicate payment ids" in f for f in run_payment_checks(dup, settle))

    cross = _leftover_payments().assign(channel_merchant_id=M2)
    assert any(
        "channel belongs" in f for f in run_payment_checks(cross, settle)
    )


def test_informational_notes_cover_the_designed_cases():
    spine, _ = _prepared_spine()
    joined = "\n".join(informational_notes(spine, _day_ops()))
    assert "1 window(s) not yet terminal" in joined  # w1 MATURED
    assert "1 REMOVED row(s)" in joined  # t3
    assert "1 HELD row(s)" in joined  # t2
    assert "1 row(s) whose transaction day differs" in joined  # t4
    assert "1 row(s) with NULL operation" in joined  # t3, pre-0009
    assert "1 successful REVERSE op(s)" in joined
    # The booked refund (has_settlement_row=True) earns no wallet note.
    assert "merchant wallet" not in joined


def test_wallet_settled_refunds_note_never_block():
    # A refund with no settlement row is settled from the merchant wallet
    # (ledger path, confirmed 2026-08-31): a note, not a gate failure.
    spine, _ = _prepared_spine()
    wallet_ops = _day_ops().assign(has_settlement_row=False)
    assert run_checks(spine) == []
    notes = informational_notes(spine, wallet_ops)
    assert any("merchant wallet" in n and "op-r1" in n for n in notes)


# --- build -----------------------------------------------------------------


def test_build_report_keeps_refund_drops_removed_and_excluded():
    spine, fee_cols = _prepared_spine()
    report = build_report(spine, pos_identifiers(_ops()), fee_cols)

    # t3 (REMOVED) and t4 (incident-excluded payment) never reach the file.
    assert set(report["settlement_transaction_id"]) == {"t1", "t2"}

    sale = report[report["settlement_transaction_id"] == "t1"].iloc[0]
    assert sale["id"] == "p1"
    assert sale["amount_sar"] == pytest.approx(12.43)
    assert sale["fee_total_sar"] == pytest.approx(0.31)
    assert sale["settled_amount_sar"] == pytest.approx(12.12)

    # The refund row references the ORIGINAL payment: same id, creation
    # date, order reference, and sale RRN — only the amounts flip sign.
    refund = report[report["settlement_transaction_id"] == "t2"].iloc[0]
    assert refund["id"] == "p1"
    assert refund["payment_creation_date"] == date(2026, 8, 19)
    assert refund["created_at_local"] == sale["created_at_local"]
    assert refund["order_reference"] == "ord-1"
    assert refund["RRN"] == sale["RRN"] == "623600000156"
    assert refund["amount_sar"] == pytest.approx(-12.43)
    assert refund["settled_amount_sar"] == pytest.approx(-12.43)
    assert refund["fee_total_sar"] == pytest.approx(0.0)
    assert refund["last_four"] == "1172"
    assert refund["card_brand"] == "MADA"


def test_build_payment_rows_leftovers_keep_empty_settlement_columns():
    report = build_payment_rows(
        _leftover_payments(), _prepared_empty_settle(), pos_identifiers(_ops())
    )
    row = report.iloc[0]
    assert row["id"] == "p2"
    assert row["amount_sar"] == pytest.approx(58.00)
    assert row["payment_creation_date"] == date(2026, 8, 19)
    # "no settlement" stays distinguishable from "zero fee", and a payment
    # without a sale receipt stays blank on RRN/STAN/TID.
    assert pd.isna(row["settlement_transaction_id"])
    assert pd.isna(row["settled_amount_sar"])
    assert pd.isna(row["fee_total_sar"])
    assert pd.isna(row["RRN"])
    assert pd.isna(row["last_four"])


def test_union_report_writes_fixed_header_per_merchant(tmp_path):
    # main()'s assembly: transaction-grain spine rows + payment-grain
    # leftovers, one file per merchant, fixed keep-list header everywhere.
    spine, fee_cols = _prepared_spine()
    pos_ids = pos_identifiers(_ops())
    report = pd.concat(
        [
            build_report(spine, pos_ids, fee_cols),
            build_payment_rows(_leftover_payments(), _prepared_empty_settle(), pos_ids),
        ],
        ignore_index=True,
    )
    files = write_daily_files(
        report, date(2026, 8, 19), date(2026, 8, 26), tmp_path, merchants=MERCHANTS
    )
    assert [(mid, n) for mid, _, n in files] == [(M1, 3), (M2, 0)]
    assert files[0][1].name == (
        "merchant_one_daily_transaction_report_2026-08-19_run_2026-08-26.csv"
    )

    m1 = pd.read_csv(files[0][1])
    assert list(m1.columns) == EXPORT_COLUMNS
    assert len(m1) == 3  # sale + refund (same payment) + leftover
    assert (m1["id"] == "p1").sum() == 2
    refund = m1[m1["settlement_status"] == "HELD"].iloc[0]
    assert refund["amount_sar"] == pytest.approx(-12.43)
    leftover = m1[m1["id"] == "p2"].iloc[0]
    assert pd.isna(leftover["settlement_transaction_id"])

    m2 = pd.read_csv(files[1][1])  # header-only file, same fixed schema
    assert len(m2) == 0
    assert list(m2.columns) == EXPORT_COLUMNS


def test_combined_file_groups_all_merchants_under_one_fixed_header(tmp_path):
    spine, fee_cols = _prepared_spine()
    pos_ids = pos_identifiers(_ops())
    m2_leftover = _leftover_payments().assign(
        id="p9", merchant_id=M2, merchant_name="Merchant Two", channel_merchant_id=M2
    )
    leftovers = pd.concat([_leftover_payments(), m2_leftover], ignore_index=True)
    report = pd.concat(
        [
            build_report(spine, pos_ids, fee_cols),
            build_payment_rows(leftovers, _prepared_empty_settle(), pos_ids),
        ],
        ignore_index=True,
    )
    path, n_rows = write_combined_file(
        report, date(2026, 8, 19), date(2026, 8, 26), tmp_path
    )
    assert path.name == (
        "all_merchants_daily_transaction_report_2026-08-19_run_2026-08-26.csv"
    )
    combined = pd.read_csv(path)
    assert n_rows == len(combined) == 4  # sale + refund + two leftovers
    assert list(combined.columns) == EXPORT_COLUMNS  # same header as delivered
    # Rows grouped by merchant: M1's three rows, then M2's one.
    assert list(combined["merchant_name"]) == ["Merchant One"] * 3 + ["Merchant Two"]


def test_export_columns_appends_only_new_fee_types():
    frame = pd.DataFrame(
        columns=[*EXPORT_COLUMNS, "fee_wht_sar", "fee_wht_minor", "n_settlement_txns"]
    )
    # New fee types are the one sanctioned header growth; minor-unit and
    # internal columns never ship.
    assert export_columns(frame) == [*EXPORT_COLUMNS, "fee_wht_sar"]


def test_merchant_slug():
    assert merchant_slug("Alatima Alraeia Company Ltd.", "x") == (
        "alatima_alraeia_company_ltd"
    )
    # Nothing slug-safe survives -> id prefix, never an empty filename.
    assert merchant_slug("شركة", "abcdef1234") == "abcdef12"
