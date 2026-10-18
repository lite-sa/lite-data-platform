"""Unit tests for the settlement report v3: pure transforms only, no
BigQuery.

Covers the cutoff, window keying (on time, late close, the strict cutoff,
open windows, the inferred end, an end moved after close), late-change
classification, the window sidecar, the notes, the writer's header, and the
comparison against v2.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import numpy as np
import pandas as pd

from app_etl.export.settlement_daily_report import EXPORT_COLUMNS
from app_etl.export.settlement_report_v3 import (
    classify_late_changes,
    compare_reports,
    cutoff_for,
    key_windows,
    window_notes,
    window_summary,
    write_file,
)
from app_etl.utils.bq_helpers import DELETED_MERCHANT_IDS

TZ = "Asia/Riyadh"


def _ts(*local: str | None) -> pd.Series:
    """Riyadh wall-clock strings -> tz-aware UTC series (None -> NaT)."""
    return pd.Series(
        pd.to_datetime(list(local)).tz_localize(TZ).tz_convert("UTC"),
        dtype="datetime64[ns, UTC]",
    )


def _windows() -> pd.DataFrame:
    # mid    midnight window, closed and loaded overnight: on time
    # five   05:00 schedule, ends 09-20 04:59:59: report day 09-20, on time
    # zad    ends 09-17, raw saw it closed 09-21 06:47: late, file 09-20
    # after  ends 09-17, raw saw it closed 09-21 07:45: late, file 09-21
    # edge   raw saw it closed exactly at the 09-21 07:30 cutoff: the cutoff
    #        is strict, so it misses file 09-20 and ships in 09-21
    # open   never seen closed: ships nowhere
    # old    no window_end anywhere: end of its collection day
    # moved  window_end moved a day after close: keyed on the end at close
    ids = ["mid", "five", "zad", "after", "edge", "open", "old", "moved"]
    return pd.DataFrame(
        {
            "settlement_window_id": ids,
            "merchant_id": ["m1"] * len(ids),
            "merchant_name": ["Merchant One"] * len(ids),
            "window_status": ["MATURED"] * 5 + ["OPENED", "SUCCESS", "MATURED"],
            "window_start": _ts(
                "2026-09-20 00:00:00",
                "2026-09-19 05:00:00",
                "2026-09-17 00:00:00",
                "2026-09-17 00:00:00",
                "2026-09-20 00:00:00",
                "2026-09-20 00:00:00",
                None,
                "2026-09-19 11:45:00",
            ),
            "window_end_at_close": _ts(
                "2026-09-20 23:59:59",
                "2026-09-20 04:59:59",
                "2026-09-17 23:59:59",
                "2026-09-17 23:59:59",
                "2026-09-20 23:59:59",
                None,
                None,
                "2026-09-20 11:44:59",
            ),
            "window_end_at_cutoff": _ts(
                "2026-09-20 23:59:59",
                "2026-09-20 04:59:59",
                "2026-09-17 23:59:59",
                "2026-09-17 23:59:59",
                "2026-09-20 23:59:59",
                "2026-09-20 23:59:59",
                None,
                "2026-09-21 11:44:59",
            ),
            "collection_day": [
                date(2026, 9, 20),
                date(2026, 9, 19),
                date(2026, 9, 17),
                date(2026, 9, 17),
                date(2026, 9, 20),
                date(2026, 9, 20),
                date(2026, 8, 14),
                date(2026, 9, 19),
            ],
            "window_settled_amount_minor": pd.array(
                [1000, 2000, 125907, 0, 500, None, 300, 700], dtype="Int64"
            ),
            "value_day": ["2026-09-22"] * len(ids),
            "closed_at": _ts(
                "2026-09-21 00:00:14",
                "2026-09-20 05:00:10",
                "2026-09-21 06:30:15",
                "2026-09-21 07:40:00",
                "2026-09-21 00:00:14",
                None,
                "2026-08-15 00:00:10",
                "2026-09-20 12:00:00",
            ),
            "closed_seen_at": _ts(
                "2026-09-21 00:45:00",
                "2026-09-20 06:47:00",
                "2026-09-21 06:47:58",
                "2026-09-21 07:45:00",
                "2026-09-21 07:30:00",
                None,
                "2026-08-15 06:50:00",
                "2026-09-20 14:40:00",
            ),
        }
    )


def _keyed() -> pd.DataFrame:
    return key_windows(_windows()).set_index("settlement_window_id")


def test_cutoff_is_next_day_build_time_in_utc():
    assert cutoff_for(date(2026, 9, 20)) == datetime(
        2026, 9, 21, 4, 30, tzinfo=timezone.utc
    )


def test_key_windows_ship_day_per_case():
    k = _keyed()
    expected = {
        "mid": (date(2026, 9, 20), False, date(2026, 9, 20)),
        "five": (date(2026, 9, 20), False, date(2026, 9, 20)),
        "zad": (date(2026, 9, 17), True, date(2026, 9, 20)),
        "after": (date(2026, 9, 17), True, date(2026, 9, 21)),
        "edge": (date(2026, 9, 20), True, date(2026, 9, 21)),
        "old": (date(2026, 8, 14), False, date(2026, 8, 14)),
        "moved": (date(2026, 9, 20), False, date(2026, 9, 20)),
    }
    for wid, (report_day, late, ship_day) in expected.items():
        row = k.loc[wid]
        assert row["report_day"] == report_day, wid
        assert row["late_close"] == late, wid
        assert row["ship_day"] == ship_day, wid


def test_key_windows_open_window_ships_nowhere():
    row = _keyed().loc["open"]
    assert pd.isna(row["ship_day"])
    assert not row["late_close"]
    assert row["report_day"] == date(2026, 9, 20)


def test_key_windows_infers_end_only_without_any_window_end():
    k = _keyed()
    assert k.loc["old", "window_end_inferred"]
    assert k.loc["old", "window_end"] == pd.Timestamp("2026-08-14 23:59:59", tz=TZ)
    assert not k.loc[["mid", "open", "moved"], "window_end_inferred"].any()


def test_window_notes_name_late_open_and_moved_end_windows():
    keyed = key_windows(_windows())
    notes = window_notes(keyed, date(2026, 9, 20), cutoff_for(date(2026, 9, 20)))
    joined = "\n".join(notes)
    assert "4 window(s) ship" in joined  # mid, five, zad, moved
    assert "1 after a late close: ['zad']" in joined
    assert "open (Merchant One, ended 2026-09-20)" in joined
    assert "window_end changed after close" in joined and "'moved'" in joined


def test_window_summary_counts_rows_and_keeps_empty_windows():
    keyed = key_windows(_windows())
    shipped = keyed.loc[keyed["ship_day"].eq(date(2026, 9, 20))]
    report = pd.DataFrame(
        {
            "settlement_window_id": ["mid", "mid", "zad"],
            "settlement_transaction_id": ["t1", "t2", "t3"],
            "settled_amount_sar": [12.0, -2.0, 1259.07],
        }
    )
    out = window_summary(shipped, report).set_index("settlement_window_id")
    assert out.loc["mid", "n_rows"] == 2
    assert out.loc["mid", "file_settled_amount_sar"] == 10.0
    assert out.loc["five", "n_rows"] == 0
    assert out.loc["zad", "late_close"]
    assert out.loc["zad", "window_settled_amount_sar"] == 1259.07
    assert out.loc["zad", "window_end_local"] == pd.Timestamp("2026-09-17 23:59:59")


def _changes() -> pd.DataFrame:
    # Shipped windows: w1, w2. w9 has not shipped.
    rows = [
        # id, prev_window, window, prev_status, status, prev_amt, amt,
        # prev_fees, fees, has_clawback
        ("added", None, "w1", None, "ACTIVE", None, 500, None, None, False),
        ("moved", "w1", "w9", "ACTIVE", "ACTIVE", 500, 500, None, None, False),
        ("reversed", "w1", "w1", "ACTIVE", "REMOVED", 900, 900, "f", "f", True),
        ("removed", "w2", "w2", "HELD", "REMOVED", -300, -300, None, None, False),
        ("restored", "w2", "w2", "REMOVED", "ACTIVE", 400, 400, None, None, False),
        ("refee", "w2", "w2", "ACTIVE", "ACTIVE", 400, 400, "a", "b", False),
        ("settled_flip", "w1", "w1", "ACTIVE", "ACTIVE", 700, 700, "f", "f", False),
        ("held_to_active", "w2", "w2", "HELD", "ACTIVE", -50, -50, None, None, False),
        ("unshipped", None, "w9", None, "ACTIVE", None, 800, None, None, False),
        ("added_removed", None, "w1", None, "REMOVED", None, 800, None, None, False),
    ]
    cols = [
        "settlement_transaction_id",
        "prev_window_id",
        "window_id",
        "prev_status",
        "status",
        "prev_amount_minor",
        "amount_minor",
        "prev_fees",
        "fees",
        "has_clawback",
    ]
    df = pd.DataFrame(rows, columns=cols)
    df["prev_amount_minor"] = df["prev_amount_minor"].astype("Int64")
    df["amount_minor"] = df["amount_minor"].astype("Int64")
    df["prev_settled_amount_minor"] = df["prev_amount_minor"]
    df["settled_amount_minor"] = df["amount_minor"]
    return df


def test_classify_late_changes_kinds():
    out = classify_late_changes(_changes(), {"w1", "w2"})
    kinds = dict(zip(out["settlement_transaction_id"], out["kind"]))
    assert kinds == {
        "added": "added",
        "moved": "moved",
        "reversed": "reversed_after_payout",
        "removed": "removed",
        "restored": "restored",
        "refee": "amount_changed",
    }


def test_classify_late_changes_empty():
    out = classify_late_changes(_changes().iloc[0:0], {"w1"})
    assert out.empty and "kind" in out.columns


def test_write_file_keeps_v2_header_plus_new_fee_types(tmp_path):
    report = pd.DataFrame(
        {c: ["x", "y"] for c in EXPORT_COLUMNS}
        | {
            "settlement_transaction_id": ["t2", "t1"],
            "merchant_name": ["B", "A"],
            "created_at": pd.to_datetime(["2026-10-09", "2026-10-09"], utc=True),
            "fee_tip_sar": [0.5, 0.25],
        }
    )
    path = tmp_path / "v3.csv"
    assert write_file(report, path) == 2
    written = pd.read_csv(path, dtype=str)
    assert list(written.columns) == [*EXPORT_COLUMNS, "fee_tip_sar"]
    assert written["merchant_name"].tolist() == ["A", "B"]


def _report(rows: list[dict]) -> pd.DataFrame:
    cols = [
        "merchant_id",
        "merchant_name",
        "id",
        "status",
        "amount_sar",
        "fee_mdr_sar",
        "settlement_transaction_id",
        "is_settled",
        "settlement_window_id",
        "window_status",
        "settled_amount_sar",
    ]
    return pd.DataFrame(rows, columns=cols).fillna("").astype(str)


def _row(
    txn,
    window,
    payment,
    merchant="m1",
    status="CAPTURED",
    fee="1.00",
    settled="99.00",
    window_status="MATURED",
) -> dict:
    return {
        "merchant_id": merchant,
        "merchant_name": "Merchant One",
        "id": payment,
        "status": status,
        "amount_sar": "100.00",
        "fee_mdr_sar": fee,
        "settlement_transaction_id": txn,
        "is_settled": "False",
        "settlement_window_id": window,
        "window_status": window_status,
        "settled_amount_sar": settled,
    }


def test_compare_reports_categories_and_reasons():
    deleted = DELETED_MERCHANT_IDS[0]
    windows = pd.DataFrame(
        {
            "settlement_window_id": ["w1", "w2", "w3", "w4", "w5"],
            "merchant_id": ["m1", "m1", "m1", deleted, "m1"],
            "ship_day": [
                date(2026, 10, 9),
                date(2026, 10, 9),
                date(2026, 10, 10),
                date(2026, 10, 9),
                np.nan,
            ],
            "collection_day": [
                date(2026, 10, 9),
                date(2026, 10, 8),
                date(2026, 10, 9),
                date(2026, 10, 9),
                date(2026, 10, 9),
            ],
            "report_day": [
                date(2026, 10, 9),
                date(2026, 10, 9),
                date(2026, 10, 10),
                date(2026, 10, 9),
                date(2026, 10, 10),
            ],
        }
    )
    v3 = _report(
        [
            _row("t1", "w1", "p1"),
            _row("t2", "w2", "p2"),
            _row("t4", "w1", "p4", fee="2.00"),
            _row("t5", "w1", "p5", status="CAPTURED"),
        ]
    )
    v2 = _report(
        [
            _row("t1", "w1", "p1", window_status="SUCCESS"),
            _row("t4", "w1", "p4", fee="1.50"),
            _row("t5", "w1", "p5", status="REFUNDED"),
            _row("t3", "w3", "p3"),
            _row("t6", "w4", "p6", merchant=deleted),
            _row("t7", "w5", "p7"),
            _row("", "", "p8", status="CAPTURED", fee="", settled=""),
            _row("", "", "p9", status="FAILED", fee="", settled=""),
        ]
    )
    lines, detail = compare_reports(v3, v2, windows, date(2026, 10, 9), {"p8"})
    got = {
        (r.category, r.settlement_transaction_id or r.id): r.reason
        for r in detail.itertuples()
    }
    assert got == {
        ("only_v3", "t2"): "v2 ships the window on collection day 2026-10-08",
        ("only_v2", "t3"): "v3 ships the window on 2026-10-10",
        ("only_v2", "t6"): "hard-deleted merchant",
        ("only_v2", "t7"): "window ends 2026-10-10, not closed at the cutoff",
        ("value_diff", "t4"): "fee_mdr_sar",
        ("v2_payment_row", "p8"): "sale booked in a window",
        ("v2_payment_row", "p9"): "no settlement row",
    }
    summary = "\n".join(lines)
    assert "rows in both: 3, 1 with a value difference" in summary
    assert "status 1" in summary and "window_status 1" in summary
    assert "windows: 1 in both, 1 only in v3, 3 only in v2" in summary


def test_compare_reports_identical_files_have_no_detail():
    windows = pd.DataFrame(
        {
            "settlement_window_id": ["w1"],
            "merchant_id": ["m1"],
            "ship_day": [date(2026, 10, 9)],
            "collection_day": [date(2026, 10, 9)],
        }
    )
    report = _report([_row("t1", "w1", "p1")])
    lines, detail = compare_reports(report, report.copy(), windows, date(2026, 10, 9))
    assert detail.empty
    assert "rows in both: 1, 0 with a value difference" in lines
