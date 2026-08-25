"""Payload-shape tests for the Slack daily summary. build_message() is
pure (mart rows + lifetime totals + month-to-date channel rows in,
Block Kit payload out), so this runs in CI with no BigQuery
credentials — same posture as test_dbt_parse.py. The freshness guard's
cutoff arithmetic is pure too and tested here.
"""

from datetime import date, datetime, timezone
from decimal import Decimal

from app_etl.notify.daily_summary import build_message, day_end_utc

ACTIVITY_DAY = date(2026, 8, 18)
TO_DATE = {"authorized_count": 8_283, "authorized_amount": Decimal("1043210.55")}
MONTH_BY_CHANNEL = [
    {
        "channel": "ecom",
        "authorized_count": 300,
        "authorized_amount": Decimal("20000.50"),
    },
    {
        "channel": "pos",
        "authorized_count": 1_000,
        "authorized_amount": Decimal("60000.00"),
    },
    # Placeholder channel: hidden as a row, still counted in the Total.
    {"channel": "unknown", "authorized_count": 5, "authorized_amount": Decimal("100")},
]


def _row(merchant_id, name=None, **overrides):
    # One mart-grain aggregate (merchant × card_brand × channel),
    # zero-filled like the mart's sums; dimensions default to the mart's
    # null placeholders.
    row = {
        "merchant_id": merchant_id,
        "merchant_name": name,
        "card_brand": "unknown",
        "channel": "unknown",
        "request_count": 0,
        "authorized_count": 0,
        "declined_count": 0,
        "gateway_declined_count": 0,
        "authorized_amount": 0,
    }
    row.update(overrides)
    return row


def _tables(payload):
    return [b for b in payload["blocks"] if b["type"] == "table"]


def test_platform_breakdowns_rates_header_and_footer():
    rows = [
        _row(
            "aaaa1111-x",
            name="Kudu",
            card_brand="MADA",
            channel="pos",
            request_count=60,
            authorized_count=55,
            declined_count=4,
            gateway_declined_count=3,
            authorized_amount=3_300,
        ),
        _row(
            "aaaa1111-x",
            name="Kudu",
            card_brand="VISA",
            channel="ecom",
            request_count=30,
            authorized_count=25,
            declined_count=4,
            gateway_declined_count=2,
            authorized_amount=1_700,
        ),
        # A wholly-placeholder slice (10 undecided payments): hidden from
        # every breakdown, still counted in the platform table.
        _row("aaaa1111-x", name="Kudu", request_count=10),
        _row(
            "bbbb2222-x",
            card_brand="MADA",
            channel="pos",
            request_count=25,
            authorized_count=10,
            declined_count=10,
            gateway_declined_count=4,
            authorized_amount=555,
        ),
    ]
    payload = build_message(rows, ACTIVITY_DAY, TO_DATE, MONTH_BY_CHANNEL)
    text = str(payload)

    # Header format is fixed by review: plain hyphen, never an em dash.
    assert "Payments Daily Summary - Tue 18 Aug 2026" in text
    assert "—" not in payload["blocks"][0]["text"]["text"]
    # Five tables, in message order: month-to-date, channel, platform,
    # card brand, top merchants.
    assert len(_tables(payload)) == 5
    # Headline numbers lead: lifetime line straight from the totals row,
    # then the month-to-date table — month named in the header column,
    # pos (larger volume) first, placeholder channel hidden but counted
    # in the Total (1,305 authorized, 80,100.50, avg 61.38); per-channel
    # avgs 60000/1000 and 20000.50/300.
    assert "Headline numbers" in text
    assert text.index("Headline numbers") < text.index("By channel")
    assert "Authorized to date" in text
    assert "8,283" in text
    assert "SAR 1,043,210.55" in text
    month_cells = str(_tables(payload)[0])
    assert "Aug 2026" in month_cells
    assert "unknown" not in month_cells
    assert month_cells.index("pos") < month_cells.index("ecom")
    assert "Total" in month_cells
    assert "1,305" in month_cells
    assert "80,100.50" in month_cells
    assert "61.38" in month_cells
    assert "60.00" in month_cells
    assert "66.67" in month_cells
    # Channel breakdown is the first daily table and carries volume +
    # avg txn: pos 3300+555 authorized volume, avg 3855/65; ecom 1700/25.
    channel_cells = str(_tables(payload)[1])
    assert "3,855.00" in channel_cells
    assert "59.31" in channel_cells
    assert "1,700.00" in channel_cells
    assert "68.00" in channel_cells
    # Platform row counts EVERYTHING, hidden placeholder rows included:
    # 125 payments = 90 authorized + 18 declined + 17 undecided; net
    # attempts 99 = 90 + 9 gateway-reached declines; gross 90/108, net
    # 90/99. The no_decision column itself is gone from display.
    assert "125" in payload["text"]
    platform_cells = str(_tables(payload)[2])
    assert "'125'" in platform_cells
    assert "'99'" in platform_cells
    assert "83.3%" in platform_cells
    assert "90.9%" in platform_cells
    assert "No decision" not in text
    assert text.index("By channel") < text.index("*Platform*")
    assert text.index("*Platform*") < text.index("By card brand")
    # Breakdowns: card brand and channel only, placeholder rows hidden,
    # sorted largest first.
    assert "By card brand" in text
    assert "By channel" in text
    assert "By processing type" not in text
    assert "By gateway" not in text
    breakdown_text = str(_tables(payload)[1]) + str(_tables(payload)[3])
    assert "unknown" not in breakdown_text
    assert "not_routed" not in breakdown_text
    assert text.index("MADA") < text.index("VISA")
    assert channel_cells.index("pos") < channel_cells.index("ecom")
    # Slice arithmetic spot-check: VISA (= ecom, one row) — gross 25/29,
    # net 25/27.
    assert "86.2%" in text
    assert "92.6%" in text
    # Top merchants ranked by authorized volume, named when the mart has
    # a name, truncated id otherwise; counts + rates per merchant (Kudu:
    # 100 payments, 8 declined, net attempts 85, gross 80/88, net 80/85)
    # and avg txn = authorized volume / authorized count (5000/80).
    assert text.index("Kudu") < text.index("bbbb2222")
    assert "bbbb2222-x" not in text
    top_cells = str(_tables(payload)[4])
    assert "'85'" in top_cells
    assert "94.1%" in top_cells
    assert "5,000.00" in top_cells
    assert "62.50" in top_cells
    assert "55.50" in top_cells
    # Fallback text carries the day's authorized volume (5000 + 555).
    assert "SAR 5,555.00" in payload["text"]
    # Footer is the four agreed bullets.
    assert "• Built on payments created 2026-08-18 (Asia/Riyadh)" in text
    assert "• Gross auth rate = authorized / total number of payments" in text
    assert "• Net auth rate = authorized / net attempts (gateway reached)" in text
    assert "• Decline failure reason breakdown coming soon ⏳" in text


def test_placeholder_only_rows_drop_breakdown_sections_and_rates_read_na():
    # A day where nothing was decided and no dimension is known, on an
    # empty mart month: the breakdown sections and the month table
    # disappear entirely (never render empty tables), and gross/net have
    # no denominator — n/a, never a fake 0%.
    rows = [_row("cccc3333-x", request_count=4)]
    payload = build_message(
        rows, ACTIVITY_DAY, {"authorized_count": 0, "authorized_amount": 0}, []
    )
    text = str(payload)

    assert len(_tables(payload)) == 2  # platform + top merchants only
    assert "Headline numbers" in text
    assert "By card brand" not in text
    assert "By channel" not in text
    assert "gross n/a, net n/a" in payload["text"]


def test_day_end_utc_is_riyadh_midnight():
    # Riyadh is UTC+3 year-round: the local day closes at 21:00 UTC, and
    # only a payment_v2 load at/after that instant can cover the full day.
    assert day_end_utc(ACTIVITY_DAY) == datetime(2026, 8, 18, 21, 0, tzinfo=timezone.utc)
