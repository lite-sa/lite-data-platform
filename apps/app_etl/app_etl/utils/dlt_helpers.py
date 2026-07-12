"""Shared dlt plumbing for the per-table ingestion pipelines: safety-lag
incremental helpers, the BigQuery/Parquet resource wrapper, and the
pipeline factory. Plain functions, not a framework — each file under
ingestion/ states only what is specific to its table.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import dlt
import sqlalchemy as sa
from dlt.destinations.adapters import bigquery_adapter

from app_etl.config import Settings

# Postgres `now()` is transaction *start* time, so a row can commit after
# the watermark has already advanced past its `created_at` — and be skipped
# forever. The extraction window's upper bound therefore trails wall clock
# by SAFETY_LAG, applied as a plain SQL predicate via `cap_upper_bound`
# (never via `incremental(end_value=...)`, which makes dlt use a mock,
# discarded state and bypasses the persisted cursor). See the README's
# watermark design for the full reasoning.
SAFETY_LAG = timedelta(minutes=10)

# Seed for the very first run only — once state exists, the persisted
# last_value takes over.
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def capped_incremental(cursor_column: str) -> dlt.sources.incremental:
    """Incremental cursor with no `end_value`; pairs with `cap_upper_bound`
    so dlt's persisted `last_value` state is never bypassed.
    """
    return dlt.sources.incremental(cursor_column, initial_value=EPOCH)


def cap_upper_bound(query: Any, table: sa.Table, incremental: Any, engine: Any) -> Any:
    """`query_adapter_callback` for `sql_table()`: adds `<cursor> <= now() -
    SAFETY_LAG`, leaving room for in-flight transactions to commit before
    their row is ever extracted.
    """
    cursor_column = incremental.cursor_path
    cutoff = sa.func.now() - sa.text(f"interval '{int(SAFETY_LAG.total_seconds())} seconds'")
    return query.where(table.c[cursor_column] <= cutoff)


def bq_resource(resource: Any) -> Any:
    """Apply `autodetect_schema=True` at the resource level: BigQuery's
    Parquet loader can't take an explicit JSON column type, which hits JSONB
    and ARRAY source columns (both reflected as dlt's abstract `json`).
    Resource-level (not destination-level) so the hint is baked into the
    schema at extract time and applies to pending packages too.
    """
    return bigquery_adapter(resource, autodetect_schema=True)


def psycopg_dsn(dsn: str) -> str:
    """Strip the `+psycopg` SQLAlchemy driver marker that dlt's sql_table()
    credentials need but psycopg.connect() doesn't understand.
    """
    return dsn.replace("postgresql+psycopg://", "postgresql://")


def bq_pipeline(pipeline_name: str, settings: Settings) -> dlt.Pipeline:
    """One dlt pipeline per source table (own name, own state), staging on
    GCS and loading into `settings.bq_dataset_raw`. The staging prefix
    mirrors the dataset name, so local test runs (BQ_DATASET_RAW=raw_test)
    can never collide with the real raw_litecore landing area.
    """
    return dlt.pipeline(
        pipeline_name=pipeline_name,
        # BigQuery defaults to the "US" multi-region, which the org policy
        # (gcp.resourceLocations, me-central2 residency) rejects. Must match
        # the region datasets/buckets were created in.
        destination=dlt.destinations.bigquery(
            project_id=settings.gcp_project, location="me-central2"
        ),
        staging=dlt.destinations.filesystem(
            bucket_url=f"gs://{settings.gcs_bucket}/{settings.bq_dataset_raw}"
        ),
        dataset_name=settings.bq_dataset_raw,
    )
