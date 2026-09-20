"""Daily payments summary posted to Slack.

Reads the `payments_daily_summary` mart for yesterday (Asia/Riyadh) and
formats it: lifetime and month-to-date headline numbers, then the day's
platform table, channel and card-brand breakdowns, and top merchants.
Breakdown rows with a placeholder dimension (unknown / not_routed) are
hidden but still count in the platform and Total rows.

gross = authorized / (authorized + declined);
net = authorized / (authorized + gateway_declined).

The job refuses to post unless a successful payment_v2 load exists at/after
the close of the local day and the mart was rebuilt at/after that load. It
runs on its own scheduler trigger; nothing retries a refusal that day.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from app_etl.config import Settings

TOP_MERCHANTS = 5
# Mirrors dbt's local_timezone var: payment_creation_date is a local
# calendar date.
LOCAL_TIMEZONE = "Asia/Riyadh"

MART_TABLE = "payments_daily_summary"

# Placeholder dimension values, compared casefolded: hidden from
# breakdowns, still counted in the platform table.
_NULL_DIMENSION_LABELS = {"unknown", "not_routed"}

_METRIC_HEADERS = (
    "Payments",
    "Authorized",
    "Declined",
    "Net attempts",
    "Gross",
    "Net",
)


def fetch_summary_rows(
    client: bigquery.Client, core: str, activity_day: date
) -> list[dict[str, Any]]:
    """One row per merchant × card_brand × channel for the activity day;
    build_message() re-aggregates per slice. Amounts are major units.
    """
    query = f"""
        select
            merchant_id,
            any_value(merchant_name) as merchant_name,
            card_brand,
            channel,
            sum(request_count) as request_count,
            sum(authorized_count) as authorized_count,
            sum(declined_count) as declined_count,
            sum(gateway_declined_count) as gateway_declined_count,
            sum(authorized_amount) as authorized_amount
        from `{core}.{MART_TABLE}`
        where payment_creation_date = @activity_day
        group by merchant_id, card_brand, channel
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("activity_day", "DATE", activity_day)
        ]
    )
    return [dict(row) for row in client.query_and_wait(query, job_config=job_config)]


def fetch_platform_totals(client: bigquery.Client, core: str) -> dict[str, Any]:
    """Lifetime authorized count and volume over the whole mart."""
    query = f"""
        select
            coalesce(sum(authorized_count), 0) as authorized_count,
            coalesce(sum(authorized_amount), 0) as authorized_amount
        from `{core}.{MART_TABLE}`
    """
    return dict(list(client.query_and_wait(query))[0])


def fetch_month_authorized_by_channel(
    client: bigquery.Client, core: str, activity_day: date
) -> list[dict[str, Any]]:
    """Authorized count and volume per channel, from the 1st of the month
    through the activity day. The upper bound keeps out the mart's partial
    current day.
    """
    query = f"""
        select
            channel,
            sum(authorized_count) as authorized_count,
            sum(authorized_amount) as authorized_amount
        from `{core}.{MART_TABLE}`
        where payment_creation_date between @month_start and @activity_day
        group by channel
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "month_start", "DATE", activity_day.replace(day=1)
            ),
            bigquery.ScalarQueryParameter("activity_day", "DATE", activity_day),
        ]
    )
    return [dict(row) for row in client.query_and_wait(query, job_config=job_config)]


def fetch_first_covering_load(
    client: bigquery.Client, raw: str, cutoff: datetime
) -> datetime | None:
    """Earliest successful payment_v2 load at/after the cutoff, from
    `_dlt_loads` (status 0 = completed). None when there is none or the
    table is missing.
    """
    query = f"""
        select min(inserted_at) as first_covering_load
        from `{raw}._dlt_loads`
        where status = 0 and schema_name = 'payment_v2' and inserted_at >= @cutoff
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("cutoff", "TIMESTAMP", cutoff)
        ]
    )
    try:
        return list(client.query_and_wait(query, job_config=job_config))[0][
            "first_covering_load"
        ]
    except NotFound:
        return None


def fetch_mart_modified(client: bigquery.Client, core: str) -> datetime | None:
    """When the mart was last rebuilt (BigQuery last-modified)."""
    try:
        return client.get_table(f"{core}.{MART_TABLE}").modified
    except NotFound:
        return None


def day_end_utc(activity_day: date) -> datetime:
    """The UTC instant at which the local activity day closed."""
    local_end = datetime.combine(
        activity_day + timedelta(days=1), time.min, tzinfo=ZoneInfo(LOCAL_TIMEZONE)
    )
    return local_end.astimezone(timezone.utc)


def _sar(amount: Any) -> str:
    # Mart amounts arrive already in major units (NUMERIC → Decimal).
    return f"SAR {Decimal(amount):,.2f}"


def _pct(rate: float | None) -> str:
    # An empty denominator has no rate: show n/a, not 0%.
    return f"{rate:.1%}" if rate is not None else "n/a"


def _merchant_label(row: dict[str, Any]) -> str:
    # merchant_name can be NULL in the mart: fall back to the id prefix.
    return row["merchant_name"] or row["merchant_id"][:8]


def _cell(text: str, bold: bool = False) -> dict[str, Any]:
    # Slack rejects empty text elements: a blank cell gets a space.
    element: dict[str, Any] = {"type": "text", "text": text or " "}
    if bold:
        element["style"] = {"bold": True}
    return {
        "type": "rich_text",
        "elements": [{"type": "rich_text_section", "elements": [element]}],
    }


def _table(
    rows: list[tuple[str, ...]],
    column_settings: list[dict[str, Any]],
    header: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """Block Kit `table` block. Slack limits: 100 rows, 20 cells per row,
    10k characters per message across tables.
    """
    all_rows = []
    if header:
        all_rows.append([_cell(c, bold=True) for c in header])
    all_rows += [[_cell(c) for c in row] for row in rows]
    return {"type": "table", "column_settings": column_settings, "rows": all_rows}


def _section(text: str) -> dict[str, Any]:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Counts, gross and net rates, authorized volume and avg txn over any
    subset of mart rows. A rate or average with an empty denominator is
    None.
    """
    m = {
        key: sum(r[key] for r in rows)
        for key in (
            "request_count",
            "authorized_count",
            "declined_count",
            "gateway_declined_count",
        )
    }
    m["authorized_amount"] = sum(
        (Decimal(r["authorized_amount"]) for r in rows), Decimal(0)
    )
    m["net_attempts"] = m["authorized_count"] + m["gateway_declined_count"]
    decided = m["authorized_count"] + m["declined_count"]
    m["gross_auth_rate"] = m["authorized_count"] / decided if decided else None
    m["net_auth_rate"] = (
        m["authorized_count"] / m["net_attempts"] if m["net_attempts"] else None
    )
    m["avg_authorized_amount"] = (
        m["authorized_amount"] / m["authorized_count"]
        if m["authorized_count"]
        else None
    )
    return m


def _avg_cell(m: dict[str, Any]) -> str:
    return (
        f"{m['avg_authorized_amount']:,.2f}"
        if m["avg_authorized_amount"] is not None
        else ""
    )


def _metric_cells(m: dict[str, Any]) -> tuple[str, ...]:
    return (
        f"{m['request_count']:,}",
        f"{m['authorized_count']:,}",
        f"{m['declined_count']:,}",
        f"{m['net_attempts']:,}",
        _pct(m["gross_auth_rate"]),
        _pct(m["net_auth_rate"]),
    )


def _rate_table(
    groups: list[tuple[str, dict[str, Any]]],
    label_header: str | None = None,
    with_volume: bool = False,
) -> dict[str, Any]:
    """One rate table. `label_header` adds a first column naming each
    group; `with_volume` adds authorized volume and avg txn columns.
    """
    right = {"align": "right"}
    headers = _METRIC_HEADERS + (
        ("Auth (SAR)", "Avg txn (SAR)") if with_volume else ()
    )

    def cells(m: dict[str, Any]) -> tuple[str, ...]:
        base = _metric_cells(m)
        if with_volume:
            base += (f"{m['authorized_amount']:,.2f}", _avg_cell(m))
        return base

    if label_header is None:
        return _table(
            [cells(m) for _, m in groups],
            [right] * len(headers),
            header=headers,
        )
    return _table(
        [(label, *cells(m)) for label, m in groups],
        [{"align": "left", "is_wrapped": True}] + [right] * len(headers),
        header=(label_header, *headers),
    )


def _breakdown(
    rows: list[dict[str, Any]], dimension: str
) -> list[tuple[str, dict[str, Any]]]:
    """Rate-table groups for one dimension, largest first, placeholders
    hidden.
    """
    groups: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        groups.setdefault(r[dimension], []).append(r)
    kept = [
        (label, _metrics(group))
        for label, group in groups.items()
        if label and label.casefold() not in _NULL_DIMENSION_LABELS
    ]
    return sorted(kept, key=lambda g: g[1]["request_count"], reverse=True)


def _month_table(
    month_rows: list[dict[str, Any]], activity_day: date
) -> dict[str, Any]:
    """Month-to-date table: authorized count / volume / avg txn by channel,
    largest volume first, plus a Total row that also counts the hidden
    placeholder channels.
    """

    def cells(label: str, count: int, amount: Decimal) -> tuple[str, ...]:
        return (
            label,
            f"{count:,}",
            f"{amount:,.2f}",
            f"{amount / count:,.2f}" if count else "",
        )

    kept = sorted(
        (
            r
            for r in month_rows
            if r["channel"] and r["channel"].casefold() not in _NULL_DIMENSION_LABELS
        ),
        key=lambda r: Decimal(r["authorized_amount"]),
        reverse=True,
    )
    body = [
        cells(r["channel"], r["authorized_count"], Decimal(r["authorized_amount"]))
        for r in kept
    ]
    body.append(
        cells(
            "Total",
            sum(r["authorized_count"] for r in month_rows),
            sum((Decimal(r["authorized_amount"]) for r in month_rows), Decimal(0)),
        )
    )
    return _table(
        body,
        [{"align": "left", "is_wrapped": True}] + [{"align": "right"}] * 3,
        header=(f"{activity_day:%b %Y}", "Authorized", "Auth (SAR)", "Avg txn (SAR)"),
    )


def build_message(
    rows: list[dict[str, Any]],
    activity_day: date,
    platform_totals: dict[str, Any],
    month_by_channel: list[dict[str, Any]],
) -> dict[str, Any]:
    """Mart rows, lifetime totals and month-to-date channel rows in, Slack
    payload out. Pure, so the unit tests cover it without BigQuery.
    """
    platform = _metrics(rows)

    per_merchant: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        per_merchant.setdefault(r["merchant_id"], []).append(r)
    merchants = [
        {
            "merchant_id": merchant_id,
            "merchant_name": group[0]["merchant_name"],
            **_metrics(group),
        }
        for merchant_id, group in per_merchant.items()
    ]
    top = sorted(merchants, key=lambda m: m["authorized_amount"], reverse=True)

    right = {"align": "right"}
    top_table = _table(
        [
            (
                str(i),
                _merchant_label(m),
                f"{m['request_count']:,}",
                f"{m['declined_count']:,}",
                f"{m['net_attempts']:,}",
                _pct(m["gross_auth_rate"]),
                _pct(m["net_auth_rate"]),
                f"{m['authorized_amount']:,.2f}",
                _avg_cell(m),
            )
            for i, m in enumerate(top[:TOP_MERCHANTS], start=1)
        ],
        [right, {"align": "left", "is_wrapped": True}] + [right] * 7,
        header=(
            "#",
            "Merchant",
            "Payments",
            "Declined",
            "Net attempts",
            "Gross",
            "Net",
            "Auth (SAR)",
            "Avg txn (SAR)",
        ),
    )

    header = f"Payments Daily Summary - {activity_day:%a %d %b %Y}"
    to_date = (
        f"Authorized to date: *{platform_totals['authorized_count']:,}* payments · "
        f"*{_sar(platform_totals['authorized_amount'])}*"
    )
    footer = "\n".join(
        [
            f"• Built on payments created {activity_day.isoformat()} "
            f"({LOCAL_TIMEZONE})",
            "• Gross auth rate = authorized / total number of payments",
            "• Net auth rate = authorized / net attempts (gateway reached)",
            "• Decline failure reason breakdown coming soon ⏳",
        ]
    )

    blocks: list[dict[str, Any]] = [
        {"type": "header", "text": {"type": "plain_text", "text": header}},
        _section("*Headline numbers*"),
        _section(to_date),
    ]
    if month_by_channel:
        blocks.append(_month_table(month_by_channel, activity_day))
    blocks.append(_section(f"*Stats for {activity_day:%a %d %b %Y}*"))
    blocks.append(_section("*Platform*"))
    blocks.append(_rate_table([("all", platform)], with_volume=True))
    
    channel_groups = _breakdown(rows, "channel")
    if channel_groups:
        blocks.append(_section("*By channel*"))
        blocks.append(_rate_table(channel_groups, label_header="", with_volume=True))
    brand_groups = _breakdown(rows, "card_brand")
    if brand_groups:
        blocks.append(_section("*By card brand*"))
        blocks.append(_rate_table(brand_groups, label_header=""))
    blocks.append(_section("*Top merchants by authorized volume*"))
    blocks.append(top_table)
    blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": footer}]})

    return {
        # Fallback for notifications/clients that don't render blocks.
        "text": (
            f"{header}: {platform['request_count']:,} payments, "
            f"{_sar(platform['authorized_amount'])} authorized "
            f"(gross {_pct(platform['gross_auth_rate'])}, "
            f"net {_pct(platform['net_auth_rate'])})"
        ),
        "blocks": blocks,
    }


def post_to_slack(webhook_url: str, payload: dict[str, Any]) -> None:
    response = requests.post(webhook_url, json=payload, timeout=30)
    # A 4xx body names the problem (invalid_payload, channel_is_archived).
    if response.status_code != 200:
        raise RuntimeError(
            f"Slack webhook returned {response.status_code}: {response.text}"
        )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the Slack payload instead of posting (no webhook needed)",
    )
    args = parser.parse_args(argv)

    settings = Settings.from_env()
    # BigQuery defaults to the US multi-region, which the org residency
    # policy rejects.
    client = bigquery.Client(project=settings.gcp_project, location="me-central2")
    raw = f"{settings.gcp_project}.{settings.bq_dataset_raw}"
    core = f"{settings.gcp_project}.{settings.bq_dataset_core}"
    activity_day = (datetime.now(ZoneInfo(LOCAL_TIMEZONE)) - timedelta(days=1)).date()

    # Freshness guard, part 1: raw covers the activity day.
    cutoff = day_end_utc(activity_day)
    first_covering_load = fetch_first_covering_load(client, raw, cutoff)
    if first_covering_load is None:
        raise SystemExit(
            f"stale raw data: no successful payment_v2 load at/after "
            f"{cutoff:%Y-%m-%d %H:%M} UTC, so raw does not cover "
            f"{activity_day} — not posting"
        )

    # Part 2: the mart was rebuilt after raw covered the day.
    mart_modified = fetch_mart_modified(client, core)
    if mart_modified is None or mart_modified < first_covering_load:
        raise SystemExit(
            f"stale mart: {MART_TABLE} last rebuilt {mart_modified}, need a "
            f"rebuild at/after the covering raw load at "
            f"{first_covering_load:%Y-%m-%d %H:%M} UTC — not posting"
        )

    rows = fetch_summary_rows(client, core, activity_day)
    if not rows:
        raise SystemExit(
            f"no payments found for {activity_day} — not posting an empty summary"
        )

    payload = build_message(
        rows,
        activity_day,
        fetch_platform_totals(client, core),
        fetch_month_authorized_by_channel(client, core, activity_day),
    )

    if args.dry_run:
        print(json.dumps(payload, indent=2))
        return
    if not settings.slack_webhook_url:
        raise SystemExit("SLACK_WEBHOOK_URL is not set (required unless --dry-run)")
    post_to_slack(settings.slack_webhook_url, payload)
    print(f"posted daily summary for payment_creation_date={activity_day}")


if __name__ == "__main__":
    main()
