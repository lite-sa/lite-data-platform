"""Generic ingestion runner — dispatches a TableConfig (see tables.py) to
one of two dlt pipeline shapes, incremental or snapshot. Adding a table
should never require a branch in this file; it means a new TableConfig.

Incremental correctness contract (unchanged from app_etl's hand-rolled
version — see that app's README for the full writeup): Postgres now() is
transaction *start* time, so a cursor column alone is not a safe
watermark — a row can commit after a later run has already advanced past
its cursor value. The guard: never extract up to now(); trail the
window's upper bound by `safety_lag_minutes` instead. dlt's own
incremental state (versioned in the destination) tracks the watermark
between runs — no hand-rolled ops.ingestion_runs table.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

import dlt
from dlt.common.pipeline import LoadInfo
from dlt.destinations.adapters import bigquery_adapter
from dlt.extract.resource import DltResource
from dlt.sources.sql_database import sql_table

from app_etl_dlt.config import Settings
from app_etl_dlt.tables import TABLES, TableConfig

CHUNK_ROWS = 50_000


def _pipeline(table_name: str, settings: Settings) -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name=f"ingest_{table_name}",
        destination=dlt.destinations.bigquery(
            project_id=settings.gcp_project, location=settings.bq_location
        ),
        staging=dlt.destinations.filesystem(bucket_url=f"gs://{settings.gcs_bucket}"),
        dataset_name=settings.bq_dataset_raw,
    )


def _apply_json_hint(resource: DltResource, table: TableConfig) -> DltResource:
    """BigQuery can't load dlt's "json" data_type from Parquet files (only
    from jsonl/model) — verified against dlt 1.28's ensure_supported_type,
    which raises rather than silently mis-loading. autodetect_schema lets
    BigQuery infer column types from the Parquet file itself instead of dlt
    pre-declaring them, which is dlt's own documented way out. Only applied
    when the table actually has json/jsonb columns, so every other table
    keeps dlt's normal (more precise) type inference."""
    if table.has_json_columns:
        return bigquery_adapter(resource, autodetect_schema=True)
    return resource


def run_incremental(table: TableConfig, settings: Settings) -> LoadInfo:
    end_value = datetime.now(timezone.utc) - timedelta(minutes=table.safety_lag_minutes)
    resource = sql_table(
        credentials=settings.pg_dsn,
        table=table.name,
        incremental=dlt.sources.incremental(
            table.cursor_column,
            initial_value=table.initial_value,
            end_value=end_value,
        ),
        chunk_size=CHUNK_ROWS,
    )
    resource = _apply_json_hint(resource, table)
    return _pipeline(table.name, settings).run(
        resource,
        table_name=table.name,
        write_disposition="append",
        primary_key=table.primary_key,
        loader_file_format="parquet",
    )


def run_snapshot(table: TableConfig, settings: Settings) -> LoadInfo:
    resource = sql_table(credentials=settings.pg_dsn, table=table.name, chunk_size=CHUNK_ROWS)
    resource = _apply_json_hint(resource, table)
    return _pipeline(table.name, settings).run(
        resource,
        table_name=table.name,
        write_disposition="replace",
        loader_file_format="parquet",
    )


def run_table(table_name: str, settings: Settings | None = None) -> LoadInfo:
    table = next((t for t in TABLES if t.name == table_name), None)
    if table is None:
        known = ", ".join(t.name for t in TABLES) or "(none configured)"
        raise ValueError(f"unknown table '{table_name}'; configured tables: {known}")

    settings = settings or Settings.from_env()
    if table.mode == "incremental":
        return run_incremental(table, settings)
    return run_snapshot(table, settings)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one table's ingestion job.")
    parser.add_argument("table", help="table name as configured in tables.py")
    args = parser.parse_args()

    load_info = run_table(args.table)
    print(load_info)


if __name__ == "__main__":
    main()
