"""Shared plumbing for the daily export reports: the local calendar, the
incident exclusion list, the latest-version dedup fragment, CLI date
handling, the raw freshness guard, and the GCS upload.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage

# Mirrors dbt's local_timezone var: report days are local calendar dates.
LOCAL_TIMEZONE = "Asia/Riyadh"

# Payments deleted or reversed upstream during incident remediation.
# Append-only raw never sees a source DELETE, so exports filter them here.
# The dbt seed incident_excluded_payments.csv is canonical; a parity test
# keeps this copy equal to it.
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
    """Subquery for the latest version per id. Raw holds one row per
    version, so a join without this fans out.
    """
    return f"""(
            select * from `{table_fqn}`
            qualify row_number() over (
                partition by id order by updated_at desc, _dlt_load_id desc
            ) = 1
        )"""


def merchant_directory(raw: str) -> str:
    """Subquery for the latest business entity per `business_id`, the key
    payment tables reference as merchant_id (not the PK `id`).
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


# The raw databases the reports read. The export job runs on its own
# schedule, outside the ingest workflow, so it checks their loads itself.
REQUIRED_EXTRACTIONS = ("payment_v2", "settlement")


def require_fresh_extraction(
    client: bigquery.Client, raw: str, report_day: date
) -> None:
    """Exit unless every REQUIRED_EXTRACTIONS database has a successful
    load in `_dlt_loads` (status 0) at/after the local close of `report_day`.
    """
    local_end = datetime.combine(
        report_day + timedelta(days=1), time.min, tzinfo=ZoneInfo(LOCAL_TIMEZONE)
    )
    cutoff = local_end.astimezone(timezone.utc)
    query = f"""
        select distinct schema_name
        from `{raw}._dlt_loads`
        where status = 0
          and schema_name in unnest(@schemas)
          and inserted_at >= @cutoff
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter(
                "schemas", "STRING", list(REQUIRED_EXTRACTIONS)
            ),
            bigquery.ScalarQueryParameter("cutoff", "TIMESTAMP", cutoff),
        ]
    )
    try:
        covered = {
            row["schema_name"]
            for row in client.query_and_wait(query, job_config=job_config)
        }
    except NotFound:
        covered = set()
    missing = sorted(set(REQUIRED_EXTRACTIONS) - covered)
    if missing:
        raise SystemExit(
            f"stale raw data: no successful {', '.join(missing)} load at/after "
            f"{cutoff:%Y-%m-%d %H:%M} UTC, so raw does not cover {report_day}, "
            f"not exporting"
        )


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
