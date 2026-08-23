"""Daily payments summary posted to Slack — the interim "morning readout"
until Metabase carries dashboards.

Reads the RAW payments table directly (latest-version dedup copied
verbatim from the staging view), NOT the payments fact or its
daily-summary mart — deliberate interim while the fact's design is still
iterating (2026-08-23); point this back at the mart when it stabilizes.
Test merchants are excluded via the dbt-seeded `<core>.test_merchants`
table, the one place their ids live outside notebooks.

`payments.status` is taken at face value — one status breakdown, no
op-derived outcome logic. The ops join was removed 2026-08-23 after
verifying on prod that the stuck-PENDING decline bug was an incident
window (payments created 2026-08-11→15, fixed upstream ~08-16), not
standing behavior. Relapse signal: declines showing up as Pending again.

The activity day is yesterday in Asia/Riyadh: payments are filtered on
created_at converted to local time (updated_at is only the version-dedup
key, never a filter). A freshness guard on dlt's load ledger refuses to
post unless a successful payment_v2 load ran after the activity day
closed — a broken ingest must yield a loud missing message, never a
quiet empty or partial one.

Runs like the transform job: no Postgres mode, no GCS bucket — a BigQuery
read plus one outbound HTTPS call. The webhook URL is the platform's first
real secret (everything else is passwordless IAM): Secret Manager +
`--set-secrets` on the Cloud Run Job, plain env var / `.env` locally.

Not wired into daily_pipeline.yaml yet — executed manually like the other
jobs. When it is wired, it goes after the ingest stage (the mart is not a
dependency any more), inside a workflow try/except that logs and
continues: a Slack outage must never mark a good data run as failed.

Known limits, deliberate for v1 (start simple, add later):
- Amounts sum across currencies (~99.5% SAR); minor→major reuses the
  fact's exp-3 divisor rule so a future BHD/KWD payment isn't 10x off.
- No refunds, chargebacks, or day-over-day deltas yet. Gross
  authorization rate = Authorized bucket / Total (created-payments
  basis, per the acceptance-metrics doc).
- Exactly three status rows (Authorized folds all capture-side
  statuses, zero-filled when absent); anything else — REQUIRES_ACTION,
  AUTHORIZATION_REVERSED, REFUNDED — counts in Total only, visible as
  the gap between Total and the rows.
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
# Mirrors dbt's local_timezone var (dbt_project.yml): the activity day is
# a local calendar date, so "yesterday" is a local-midnight question.
LOCAL_TIMEZONE = "Asia/Riyadh"

# Bucket spec (2026-08-23 review): the funnel shows exactly three status
# rows — Authorized folds every capture-side status, Pending and Failed
# map 1:1. Enum casing verified on prod raw: UPPERCASE (the data
# dictionary doc lists lowercase; PARTIAL/FINAL_CAPTURED are pre-mapped
# but never observed yet). Statuses outside the buckets
# (REQUIRES_ACTION, AUTHORIZATION_REVERSED, REFUNDED) count in Total
# only — visible as the gap between Total and the three rows.
_AUTH_STATUSES = ("AUTHORIZED", "CAPTURED", "PARTIAL_CAPTURED", "FINAL_CAPTURED")


def fetch_summary_rows(
    client: bigquery.Client, raw: str, core: str, activity_day: date
) -> list[dict[str, Any]]:
    """Merchant × status rows for the one activity day, from the payments
    table alone (latest version per id — the staging view's dedup contract
    copied verbatim; raw is append-only, one row per (id, updated_at)
    version). Names come from raw business_entities, same dedup.
    """
    minor_per_major = (
        "if(payments.currency in "
        "('BHD', 'IQD', 'JOD', 'KWD', 'LYD', 'OMR', 'TND'), 1000, 100)"
    )
    query = f"""
        with
        payments as (
            select id, merchant_id, status, amount, currency, created_at
            from `{raw}.payment_v2__payments`
            qualify row_number() over (
                partition by id order by updated_at desc, _dlt_load_id desc
            ) = 1
        ),
        merchants as (
            select business_id as merchant_id, name as merchant_name
            from `{raw}.business_management__business_entities`
            where business_id is not null
            qualify row_number() over (
                partition by business_id
                order by updated_at desc, _dlt_load_id desc
            ) = 1
        )
        select
            payments.merchant_id,
            any_value(merchants.merchant_name) as merchant_name,
            payments.status,
            count(*) as payment_count,
            sum(cast(payments.amount as numeric) / {minor_per_major}) as amount
        from payments
        left join merchants on payments.merchant_id = merchants.merchant_id
        where
            date(datetime(payments.created_at, '{LOCAL_TIMEZONE}')) = @activity_day
            and payments.merchant_id not in (
                select merchant_id from `{core}.test_merchants`
            )
        group by payments.merchant_id, payments.status
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("activity_day", "DATE", activity_day)
        ]
    )
    return [dict(row) for row in client.query_and_wait(query, job_config=job_config)]


def fetch_last_payment_load(client: bigquery.Client, raw: str) -> datetime | None:
    """Last successful payment_v2 load from dlt's own ledger (status 0 =
    completed). Missing table (e.g. a fresh dataset) → None, which the
    guard treats as stale.
    """
    query = f"""
        select max(inserted_at) as last_load
        from `{raw}._dlt_loads`
        where status = 0 and schema_name = 'payment_v2'
    """
    try:
        return list(client.query_and_wait(query))[0]["last_load"]
    except NotFound:
        return None


def day_end_utc(activity_day: date) -> datetime:
    """The UTC instant at which the local activity day closed — only an
    ingestion run at/after this moment can have seen the whole day.
    """
    local_end = datetime.combine(
        activity_day + timedelta(days=1), time.min, tzinfo=ZoneInfo(LOCAL_TIMEZONE)
    )
    return local_end.astimezone(timezone.utc)


def _sar(amount: Any) -> str:
    # Query amounts arrive already converted to major units (NUMERIC → Decimal).
    return f"SAR {Decimal(amount):,.2f}"


def _merchant_label(row: dict[str, Any]) -> str:
    # merchant_name is a raw-side left join — degrade to truncated id.
    return row["merchant_name"] or row["merchant_id"][:8]


def _cell(text: str, bold: bool = False) -> dict[str, Any]:
    # Slack requires non-empty text elements — a blank cell gets a space.
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
    """Native Block Kit `table` block — the client owns the layout, so it
    reflows on mobile (a box-drawn code-block table was tried 2026-08-23
    and wrapped badly there). Limits far above our scale: 100 rows, 20
    cells/row, 10k chars per message summed across tables.
    """
    all_rows = []
    if header:
        all_rows.append([_cell(c, bold=True) for c in header])
    all_rows += [[_cell(c) for c in row] for row in rows]
    return {"type": "table", "column_settings": column_settings, "rows": all_rows}


def build_message(rows: list[dict[str, Any]], activity_day: date) -> dict[str, Any]:
    """Pure: merchant × status rows in, Slack payload out — this is what
    the unit test covers, no BigQuery involved.
    """
    by_status: dict[str, list[Any]] = {}
    per_merchant: dict[str, dict[str, Any]] = {}
    for r in rows:
        count, amount = by_status.setdefault(r["status"], [0, Decimal(0)])
        by_status[r["status"]] = [count + r["payment_count"], amount + Decimal(r["amount"])]
        m = per_merchant.setdefault(
            r["merchant_id"],
            {
                "merchant_id": r["merchant_id"],
                "merchant_name": r["merchant_name"],
                "request_count": 0,
                "auth_amount": Decimal(0),
            },
        )
        m["request_count"] += r["payment_count"]
        if r["status"] in _AUTH_STATUSES:
            m["auth_amount"] += Decimal(r["amount"])

    total_count = sum(c for c, _ in by_status.values())
    total_amount = sum((a for _, a in by_status.values()), Decimal(0))

    def bucket(statuses: tuple[str, ...]) -> tuple[int, Decimal]:
        counts = [by_status.get(s, (0, Decimal(0))) for s in statuses]
        return (
            sum(c for c, _ in counts),
            sum((a for _, a in counts), Decimal(0)),
        )

    authorized_count, authorized_amount = bucket(_AUTH_STATUSES)
    pending_count, pending_amount = bucket(("PENDING",))
    failed_count, failed_amount = bucket(("FAILED",))
    gross_auth_rate = authorized_count / total_count

    right = {"align": "right"}
    funnel_table = _table(
        [
            ("Total", f"{total_count:,}", _sar(total_amount)),
            ("Authorized", f"{authorized_count:,}", _sar(authorized_amount)),
            ("Pending", f"{pending_count:,}", _sar(pending_amount)),
            ("Failed", f"{failed_count:,}", _sar(failed_amount)),
        ],
        [{"align": "left"}, right, right],
        header=("", "Count", "Volume"),
    )

    top = sorted(per_merchant.values(), key=lambda m: m["auth_amount"], reverse=True)
    top_table = _table(
        [
            (
                str(i),
                _merchant_label(m),
                f"{m['auth_amount']:,.2f}",
                f"{m['request_count']:,}",
            )
            for i, m in enumerate(top[:TOP_MERCHANTS], start=1)
        ],
        [right, {"align": "left", "is_wrapped": True}, right, right],
        header=("#", "Merchant", "Auth (SAR)", "Payments"),
    )

    header = f"Payments Daily Summary - {activity_day:%a %d %b %Y}"
    footer = (
        f"Built on payments created {activity_day.isoformat()} ({LOCAL_TIMEZONE})."
    )
    return {
        # Fallback for notifications/clients that don't render blocks.
        "text": (
            f"{header}: {total_count:,} payments, "
            f"{_sar(authorized_amount)} authorized ({gross_auth_rate:.1%})"
        ),
        "blocks": [
            {"type": "header", "text": {"type": "plain_text", "text": header}},
            funnel_table,
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"Gross authorization rate: *{gross_auth_rate:.1%}*",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Top merchants by authorized volume*",
                },
            },
            top_table,
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": footer}],
            },
        ],
    }


def post_to_slack(webhook_url: str, payload: dict[str, Any]) -> None:
    response = requests.post(webhook_url, json=payload, timeout=30)
    # Slack answers 200 "ok" on success; 4xx bodies name the problem
    # (invalid_payload, channel_is_archived, …) — surface them.
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
    # Explicit location, as everywhere: BigQuery defaults to the US
    # multi-region, which the org residency policy rejects.
    client = bigquery.Client(project=settings.gcp_project, location="me-central2")
    raw = f"{settings.gcp_project}.{settings.bq_dataset_raw}"
    core = f"{settings.gcp_project}.{settings.bq_dataset_core}"
    activity_day = (datetime.now(ZoneInfo(LOCAL_TIMEZONE)) - timedelta(days=1)).date()

    # Freshness guard: a failed or late ingest must yield a loud missing
    # message, never a quiet empty or partial one.
    last_load = fetch_last_payment_load(client, raw)
    cutoff = day_end_utc(activity_day)
    if last_load is None or last_load < cutoff:
        raise SystemExit(
            f"stale raw data: last successful payment_v2 load is "
            f"{last_load}, need one at/after {cutoff:%Y-%m-%d %H:%M} UTC "
            f"to cover {activity_day} — not posting"
        )

    rows = fetch_summary_rows(client, raw, core, activity_day)
    if not rows:
        raise SystemExit(
            f"no payments found for {activity_day} — not posting an empty summary"
        )

    payload = build_message(rows, activity_day)

    if args.dry_run:
        print(json.dumps(payload, indent=2))
        return
    if not settings.slack_webhook_url:
        raise SystemExit("SLACK_WEBHOOK_URL is not set (required unless --dry-run)")
    post_to_slack(settings.slack_webhook_url, payload)
    print(f"posted daily summary for payment_creation_date={activity_day}")


if __name__ == "__main__":
    main()
