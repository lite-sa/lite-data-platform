"""Payload-shape tests for the Slack daily summary. build_message() is
pure (merchant × status rows in, Block Kit payload out), so this runs in
CI with no BigQuery credentials — same posture as test_dbt_parse.py. The
freshness guard's cutoff arithmetic is pure too and tested here.
"""

from datetime import date, datetime, timezone

from app_etl.notify.daily_summary import build_message, day_end_utc

ACTIVITY_DAY = date(2026, 8, 18)


def _row(merchant_id, status, count, amount, name=None):
    # One merchant × status aggregate, amounts in major units.
    return {
        "merchant_id": merchant_id,
        "merchant_name": name,
        "status": status,
        "payment_count": count,
        "amount": amount,
    }


def test_totals_buckets_rate_header_and_footer():
    rows = [
        _row("aaaa1111-x", "CAPTURED", 90, 5_000, name="Kudu"),
        _row("aaaa1111-x", "PARTIAL_CAPTURED", 10, 500, name="Kudu"),
        _row("bbbb2222-x", "PENDING", 14, 300),
        _row("bbbb2222-x", "FAILED", 10, 400),
        _row("bbbb2222-x", "AUTHORIZATION_REVERSED", 1, 50),
    ]
    payload = build_message(rows, ACTIVITY_DAY)
    text = str(payload)

    # Header format is fixed by review: plain hyphen, never an em dash.
    assert "Payments Daily Summary - Tue 18 Aug 2026" in text
    assert "—" not in payload["blocks"][0]["text"]["text"]
    # Totals: 125 payments / SAR 6,250.00 across ALL statuses (the
    # reversed payment counts in Total only); Authorized folds the
    # capture-side statuses: 100 payments / SAR 5,500.00.
    assert "125" in payload["text"]
    assert "SAR 6,250.00" in text
    assert "SAR 5,500.00" in text
    assert "Authorized" in text
    # Gross authorization rate = Authorized / Total = 100/125.
    assert "Gross authorization rate" in text
    assert "80.0%" in text
    # Exactly the three bucket rows — no per-status rows any more.
    assert "Captured" not in text
    assert "Authorization reversed" not in text
    # Funnel and top merchants render as native Block Kit table blocks.
    assert sum(b["type"] == "table" for b in payload["blocks"]) == 2
    # Top merchants ranked by capture-side volume (5,500 > 0; the
    # reversed 50 doesn't count); named when raw has a name, truncated
    # id otherwise.
    assert text.index("Kudu") < text.index("bbbb2222")
    assert "bbbb2222-x" not in text
    # Footer says only what the data is built on.
    assert "Built on payments created 2026-08-18" in text


def test_day_end_utc_is_riyadh_midnight():
    # Riyadh is UTC+3 year-round: the local day closes at 21:00 UTC, and
    # only a payment_v2 load at/after that instant can cover the full day.
    assert day_end_utc(ACTIVITY_DAY) == datetime(2026, 8, 18, 21, 0, tzinfo=timezone.utc)
