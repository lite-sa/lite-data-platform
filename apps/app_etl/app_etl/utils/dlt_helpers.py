"""Shared dlt plumbing for the per-database ingestion pipelines: safety-lag
cursor helpers, the BigQuery resource wrapper, source credentials, and the
pipeline factory.
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

# Postgres `now()` is transaction start time, so a row can commit after the
# cursor has passed it. The extraction window therefore ends SAFETY_LAG
# behind wall clock (see `cap_upper_bound`).
SAFETY_LAG = timedelta(minutes=10)

# Cursor seed for a pipeline's first run.
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def capped_incremental(cursor_column: str) -> dlt.sources.incremental:
    """Incremental cursor for `sql_table()`. No `end_value`: that would make
    dlt skip its persisted cursor. `cap_upper_bound` sets the upper bound.
    """
    return dlt.sources.incremental(cursor_column, initial_value=EPOCH)


def cap_upper_bound(query: Any, table: sa.Table, incremental: Any, engine: Any) -> Any:
    """`query_adapter_callback` for `sql_table()`: adds
    `<cursor> <= now() - SAFETY_LAG` to the extraction query.
    """
    cursor_column = incremental.cursor_path
    cutoff = sa.func.now() - sa.text(f"interval '{int(SAFETY_LAG.total_seconds())} seconds'")
    return query.where(table.c[cursor_column] <= cutoff)


def bq_resource(
    resource: Any, partition: str | None = None, cluster: str | list[str] | None = None
) -> Any:
    """BigQuery hints for a resource. `autodetect_schema` lets BigQuery infer
    types, because its Parquet loader rejects an explicit JSON type (JSONB
    and ARRAY source columns).

    `partition` day-partitions on that column; `cluster` takes up to 4
    columns. BigQuery fixes both at table creation: changing either needs
    one run with `--refresh`.
    """
    return bigquery_adapter(
        resource, autodetect_schema=True, partition=partition, cluster=cluster
    )


def refresh_mode(argv: list[str] | None = None) -> TRefreshMode | None:
    """Parse `--refresh`: drop this pipeline's destination tables and cursor
    state together, then reload from EPOCH. A CLI flag, not an env var, so it
    applies to one execution only. On Cloud Run pass the full args list:
    `--args="-m,app_etl.ingestion.<db>,--refresh"`.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="drop this pipeline's destination tables and cursor state, then reload from scratch",
    )
    return "drop_resources" if parser.parse_args(argv).refresh else None


def pg_credentials(settings: Settings, db: str) -> str | sa.engine.Engine:
    """Credentials for `sql_table(credentials=...)` on database `db`.

    Proxy mode returns a DSN for the Cloud SQL Auth Proxy on localhost.
    Instance mode returns an Engine backed by the Cloud SQL Python Connector
    with IAM auth. The Connector is never closed: it ends with the process.
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
            ip_type={
                "private": IPTypes.PRIVATE,
                "public": IPTypes.PUBLIC,
                "psc": IPTypes.PSC,
            }[settings.pg_ip_type],
        )

    return sa.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
        # IAM tokens live 60 min; recycle pooled connections before that.
        pool_recycle=1800,
    )


def bq_pipeline(pipeline_name: str, settings: Settings) -> dlt.Pipeline:
    """dlt pipeline for one source database: GCS staging, BigQuery
    destination, dataset `settings.bq_dataset_raw`.

    The staging prefix is `<dataset>/<pipeline_name>`, so test datasets and
    concurrent pipelines never share a staging folder (dlt truncates a
    table's staging folder before each load). The `_dlt_*` state tables in
    BigQuery are shared: BigQuery allows 5 writes per 10 s per table, so the
    daily workflow runs the ingest jobs one after another.
    """
    if not settings.gcs_bucket:
        raise ValueError("GCS_BUCKET is required for ingestion staging")
    return dlt.pipeline(
        pipeline_name=pipeline_name,
        # BigQuery defaults to the US multi-region, which the org residency
        # policy rejects.
        destination=dlt.destinations.bigquery(
            project_id=settings.gcp_project, location="me-central2"
        ),
        staging=dlt.destinations.filesystem(
            bucket_url=f"gs://{settings.gcs_bucket}/{settings.bq_dataset_raw}/{pipeline_name}"
        ),
        dataset_name=settings.bq_dataset_raw,
    )
