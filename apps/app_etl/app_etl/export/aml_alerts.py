"""Daily AML alert egress: one JSONL file per scenario plus a manifest,
under `alerts/dt=<run_date>/` in the egress bucket. The contract lives in
docs/aml-alert-design.md — this module is its implementation, not its
definition.

Stateless by design: suppression already landed in SQL, so "new alerts" is
simply `run_date = <today> and not is_suppressed`, and a same-day rerun
overwrites the folder's objects under the same names. The one thing this
job cannot know is whether dbt actually evaluated run_date — an export for
a day dbt never built would write a lying "evaluated, nothing raised" empty
file. The daily workflow guarantees ordering (ingest → dbt build → export);
manual runs inherit that responsibility.

Run as `python -m app_etl.export.aml_alerts [--run-date YYYY-MM-DD]` — the
Cloud Run job takes the default (today, business-local); `--run-date` is
for re-delivering a specific day after an outage.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from google.cloud import bigquery, storage

from app_etl.config import Settings

# Mirrors dbt's aml_local_timezone var: run_date is a business-local
# calendar date, and the workflow runs ~06:45 local — well past the night
# window close, so "today" here is the run_date dbt just built.
LOCAL_TIMEZONE = "Asia/Riyadh"

# Active scenarios: scenario_id -> its mart table (one table per scenario,
# docs/aml-alert-design.md). Adding a scenario = its dbt model + one entry
# here. Every listed scenario writes its file every day, even empty — an
# empty file means "evaluated, nothing raised"; an absent file means "not
# evaluated (yet)".
SCENARIOS = {
    "AML-008": "aml_alerts_008",
    "AML-014": "aml_alerts_014",
    "AML-015": "aml_alerts_015",
}

MANIFEST_NAME = "_MANIFEST.json"


def _json_value(value: object) -> object:
    """`json.dumps` default= hook for the BigQuery value types JSON lacks."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    raise TypeError(f"unexpected BigQuery value type {type(value).__name__}")


def to_jsonl(rows: list[dict]) -> str:
    """One JSON object per line; key order follows the table's column order
    (the fixed spine — scenario-specific data only inside `evidence`), so
    files diff cleanly across reruns."""
    return "".join(
        json.dumps(row, default=_json_value, ensure_ascii=False) + "\n" for row in rows
    )


def fetch_alerts(bq: bigquery.Client, table_fqn: str, run_date: date) -> list[dict]:
    """The day's unsuppressed rows, the suppression block dropped: every
    delivered record is actionable by contract, so an always-false flag
    (and its always-NULL companions) would only invite consumers to build
    filters they don't need."""
    job = bq.query(
        f"""
        select * except (
            suppression_period_start,
            suppression_period_end,
            suppressed_by_alert_id,
            is_suppressed
        )
        from `{table_fqn}`
        where run_date = @run_date and not is_suppressed
        order by alert_id
        """,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("run_date", "DATE", run_date)]
        ),
    )
    return [dict(row) for row in job.result()]


def export_alerts(run_date: date | None = None) -> None:
    settings = Settings.from_env()
    if not settings.gcs_bucket_egress:
        raise ValueError("GCS_BUCKET_EGRESS is required for the alert export job")
    if run_date is None:
        run_date = datetime.now(ZoneInfo(LOCAL_TIMEZONE)).date()

    bq = bigquery.Client(project=settings.gcp_project, location="me-central2")
    bucket = storage.Client(project=settings.gcp_project).bucket(settings.gcs_bucket_egress)
    prefix = f"alerts/dt={run_date.isoformat()}"

    manifest_scenarios = []
    for scenario_id, table in sorted(SCENARIOS.items()):
        table_fqn = f"{settings.gcp_project}.{settings.bq_dataset_aml}.{table}"
        rows = fetch_alerts(bq, table_fqn, run_date)
        file_name = f"{scenario_id}.jsonl"
        bucket.blob(f"{prefix}/{file_name}").upload_from_string(
            to_jsonl(rows), content_type="application/x-ndjson"
        )
        manifest_scenarios.append(
            {"scenario_id": scenario_id, "file": file_name, "alert_count": len(rows)}
        )
        print(f"{scenario_id}: {len(rows)} alert(s) -> gs://{settings.gcs_bucket_egress}/{prefix}/{file_name}")

    # Written last on purpose: the manifest is the day's commit marker —
    # consumers pick a day up only once it exists and validate their parsed
    # counts against it.
    manifest = {
        "run_date": run_date.isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scenarios": manifest_scenarios,
    }
    bucket.blob(f"{prefix}/{MANIFEST_NAME}").upload_from_string(
        json.dumps(manifest, indent=2) + "\n", content_type="application/json"
    )
    print(f"manifest -> gs://{settings.gcs_bucket_egress}/{prefix}/{MANIFEST_NAME}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Export the day's unsuppressed AML alerts to the egress bucket")
    parser.add_argument(
        "--run-date",
        type=date.fromisoformat,
        default=None,
        help="run_date to export (default: today in the business timezone)",
    )
    export_alerts(parser.parse_args(argv).run_date)


if __name__ == "__main__":
    main()
