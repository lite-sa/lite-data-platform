"""Shared plumbing for the daily export reports: the Riyadh calendar
contract, the incident exclusion list, the latest-version dedup fragment,
CLI/date handling, and the GCS upload. Report-specific logic (queries,
column layouts, filenames) stays in the report modules — this module must
never need touching to add a report.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from google.cloud import storage

# Mirrors dbt's local_timezone var: report days are local calendar dates,
# and "previous day" is a local-midnight question.
LOCAL_TIMEZONE = "Asia/Riyadh"

# Payments hand-deleted or ledger-reversed upstream during incident
# remediation (first case: inc-2026-08-24-duplicate-webhooks). Append-only
# raw never sees a source DELETE, so without this filter the rows resurface
# in exported files — the reversed 60 SAR capture shipped in alquwa
# almuttalaqa's 2026-08-24 file exactly this way. Canonical list is the
# dbt seed incident_excluded_payments.csv; a parity test keeps this copy
# in lockstep (the exports read raw directly, and the DAG runs them in
# parallel with dbt build, so they cannot depend on the seed table).
EXCLUDED_PAYMENT_IDS = [
    "d565fbdc-e26a-458a-9cc2-b84b81e031c3",
    "92d01a69-3c74-4b80-87a8-5250f5abb7ad",
    "6b0f20bb-5176-4d04-8745-d4f639ec2675",
    "aeca7623-7224-4221-b02f-ba6164ec0275",
    "aa6c4bf8-48e0-4b39-8466-d8a1bbc26e3d",
    "84e62d43-85e5-45a4-a36b-c224127a3724",
    "5bd8ba25-72b0-4724-ac3a-d12a2dc43830",
    "170e25c6-1072-42b2-a7e1-a42957d32717",
    "ac07fcb0-136a-4be0-a50b-8392481fe702",
]


def latest_version(table_fqn: str) -> str:
    """Subquery selecting the latest version per id — the dedup a stg_
    model would own. Raw is append-only (one row per (id, updated_at)
    version), so every reader must come through this or joins fan out.
    """
    return f"""(
            select * from `{table_fqn}`
            qualify row_number() over (
                partition by id order by updated_at desc, _dlt_load_id desc
            ) = 1
        )"""


def merchant_directory(raw: str) -> str:
    """Subquery for the latest business entity per business_id — the
    NATURAL key the payment tables reference as merchant_id, not id (the
    source PK), so this is deliberately not latest_version(). A no-op
    under the current replace disposition; becomes real once snapshot
    history accumulates.
    """
    return f"""(
            select business_id, name
            from `{raw}.business_management__business_entities`
            where business_id is not null
            qualify row_number() over (
                partition by business_id order by updated_at desc, _dlt_load_id desc
            ) = 1
        )"""


def make_arg_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=None,
        help="report day (Riyadh calendar); default: yesterday",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="write local files, skip the GCS upload (no bucket needed)",
    )
    return parser


def resolve_days(args: argparse.Namespace) -> tuple[date, date]:
    """(report_day, run_date), both Riyadh calendar."""
    run_date = datetime.now(ZoneInfo(LOCAL_TIMEZONE)).date()
    return args.date or run_date - timedelta(days=1), run_date


def upload_files(
    pairs: list[tuple[str, Path]], bucket_name: str, project: str
) -> list[str]:
    """Upload (blob_name, local_path) pairs; returns the gs:// URIs."""
    client = storage.Client(project=project)
    bucket = client.bucket(bucket_name)
    uris: list[str] = []
    for blob_name, path in pairs:
        bucket.blob(blob_name).upload_from_filename(path)
        uris.append(f"gs://{bucket_name}/{blob_name}")
    return uris
