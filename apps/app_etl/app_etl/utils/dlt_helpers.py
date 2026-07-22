"""Shared dlt plumbing for the per-database ingestion pipelines: safety-lag
incremental helpers, the BigQuery/Parquet resource wrapper, and the
pipeline factory. Plain functions, not a framework — each file under
ingestion/ states only what is specific to its database and tables.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from typing import Any

import dlt
import sqlalchemy as sa
from dlt.common.pipeline import TRefreshMode
from dlt.destinations.adapters import bigquery_adapter
from google.cloud.sql.connector import Connector, IPTypes

from app_etl.config import Settings

# Postgres `now()` is transaction *start* time, so a row can commit after
# the watermark has already advanced past its cursor value — and be skipped
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


def bq_resource(
    resource: Any, partition: str | None = None, cluster: str | list[str] | None = None
) -> Any:
    """Apply `autodetect_schema=True` at the resource level: BigQuery's
    Parquet loader can't take an explicit JSON column type, which hits JSONB
    and ARRAY source columns (both reflected as dlt's abstract `json`).
    Resource-level (not destination-level) so the hint is baked into the
    schema at extract time and applies to pending packages too. Applied
    uniformly rather than only to json-bearing tables: one schema path, at
    the cost of BQ inferring types instead of dlt declaring them — revisit
    if a table ever needs dlt's exact types.

    `partition` day-partitions the destination table on that column (a
    timestamp column becomes BigQuery DAY time-partitioning). `cluster`
    sets BigQuery clustering keys (up to 4 columns). Both are immutable at
    CREATE: changing either on an existing table needs a drop + full
    reload — run the pipeline once with `--refresh` (see `refresh_mode`).
    """
    return bigquery_adapter(
        resource, autodetect_schema=True, partition=partition, cluster=cluster
    )


def refresh_mode(argv: list[str] | None = None) -> TRefreshMode | None:
    """Entry-point flag shared by every pipeline: `--refresh` maps to dlt's
    `refresh="drop_resources"`, which drops the resource's destination
    tables *and* its persisted cursor state together, so the run re-extracts
    from EPOCH. (Dropping only the BQ table would leave the watermark
    behind and silently skip history.) A CLI arg rather than an env var so
    it can't linger in a `.env` or Cloud Run job config — pass it per
    execution, e.g. `gcloud run jobs execute ... --args=--refresh`. Needed
    whenever a create-time-only BigQuery property (partitioning,
    clustering) changes on an existing table.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="drop this pipeline's destination tables and cursor state, then reload from scratch",
    )
    return "drop_resources" if parser.parse_args(argv).refresh else None


def pg_credentials(settings: Settings, db: str) -> str | sa.engine.Engine:
    """What every pipeline passes to `sql_table(credentials=...)`. `db` is
    the pipeline's source database — one pipeline per database, so it lives
    in the pipeline file, never in the env.

    Proxy mode returns a DSN string pointing at the Cloud SQL Auth Proxy on
    localhost. Instance mode returns an Engine whose connections come from
    the Cloud SQL Python Connector with IAM database auth: the connector
    resolves the instance via the Admin API (billed to the ADC quota project
    — see docs/gcp-auth-and-config.md), opens an mTLS tunnel, and mints a
    per-connection OAuth token as the password. The Connector is
    deliberately never closed — these are batch jobs; it dies with the
    process.
    """
    if settings.pg_host:
        return settings.pg_dsn(db)
    if not settings.pg_instance:
        raise ValueError(
            "no source connection configured: ingestion needs PG_HOST/PG_PORT/"
            "PG_USER (Auth Proxy) or PG_INSTANCE_CONNECTION_NAME + PG_IAM_USER "
            "(Cloud Run) — only transform/export jobs run without one"
        )

    connector = Connector()

    def getconn():
        return connector.connect(
            settings.pg_instance,
            "pg8000",  # the connector has no psycopg support; pg8000 is its Postgres driver
            user=settings.pg_iam_user,
            db=db,
            enable_iam_auth=True,
            ip_type=IPTypes.PRIVATE if settings.pg_ip_type == "private" else IPTypes.PUBLIC,
        )

    return sa.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
        # IAM tokens live 60min; recycling under that keeps every pooled
        # connection younger than its auth window on long extracts.
        pool_recycle=1800,
    )


def bq_pipeline(pipeline_name: str, settings: Settings) -> dlt.Pipeline:
    """One dlt pipeline per source database (own name, own state — a
    pipeline's resources share one connection), staging on GCS and loading
    into `settings.bq_dataset_raw`. The staging prefix mirrors the dataset
    name, so local test runs (BQ_DATASET_RAW=raw_test) can never collide
    with the real raw_litecore landing area.
    """
    if not settings.gcs_bucket:
        raise ValueError("GCS_BUCKET is required for ingestion staging")
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
