"""Shared plumbing for the local_source_test disposable pipelines (env
loading, the safety-lag incremental helper, the BigQuery/Parquet resource
wrapper). Not a framework -- kept small on purpose since this whole folder
is throwaway (see README.md).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import dlt
import sqlalchemy as sa
from dlt.destinations.adapters import bigquery_adapter

# `created_at` is set by Postgres `now()` at transaction *start*, not commit --
# a long-running txn can commit after a shorter one that started later, so a
# naive "extract everything > last_value" can permanently skip a row if the
# watermark advances past its now()-based timestamp before it commits.
#
# First attempt here used `dlt.sources.incremental(end_value=now()-SAFETY_LAG)`
# to cap the window's upper edge -- wrong. dlt's incremental uses a *mock,
# discarded* state whenever `end_value` is set (dlt/extract/incremental/
# __init__.py get_state(): "If end_value is set, a mock state is created that
# will be discarded after extract step"). The real persisted last_value is
# never read or written in that mode, so every run restarted from
# `initial_value` and re-appended the whole window -- confirmed 2026-07-12
# after payment counts kept growing on reruns with no new seed data.
#
# Second attempt used `lag=` to rewind the *start* of each window and
# re-fetch a trailing overlap, dedup'd via merge + primary_key. That works,
# but payments/payment_operations rows are insert-once and never mutated in
# place (confirmed 2026-07-12), so there is nothing to merge -- the overlap
# is pure re-fetch of identical rows, paying MERGE compute for zero benefit.
# app_etl/README.md's watermark design already named the right tool for this:
# cap the query's upper bound "via the sql_database source's query adapter"
# rather than via the incremental object's `end_value`. `query_adapter_
# callback` (below) adds `created_at <= now() - SAFETY_LAG` as a plain SQL
# predicate; the incremental itself keeps `end_value=None`, so dlt's real
# state persists and `last_value` only ever advances to a point already
# guaranteed committed -- append-only, no merge, no dedup contract.
SAFETY_LAG = timedelta(minutes=10)

# Seed for the very first run only -- once state exists, the persisted
# last_value takes over regardless of what this says.
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def load_dotenv(path: Path | None = None) -> None:
    """Minimal .env loader -- existing env vars always win."""
    path = path or Path(__file__).parent / ".env"
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


@dataclass(frozen=True)
class Env:
    gcp_project: str
    gcs_bucket: str
    pg_dsn: str


def load_env() -> Env:
    load_dotenv()
    return Env(
        gcp_project=os.environ["GCP_PROJECT"],
        gcs_bucket=os.environ["GCS_BUCKET"],
        pg_dsn=os.environ.get(
            "PG_DSN", "postgresql+psycopg://dlt_test:dlt_test@localhost:5432/litecore_test"
        ),
    )


def capped_incremental(cursor_column: str) -> dlt.sources.incremental:
    """Plain incremental with no `end_value` -- the upper-bound cap is
    applied separately, via `cap_upper_bound` as a `query_adapter_callback`,
    so dlt's persisted `last_value` state is never bypassed.
    """
    return dlt.sources.incremental(cursor_column, initial_value=EPOCH)


def cap_upper_bound(query: Any, table: sa.Table, incremental: Any, engine: Any) -> Any:
    """`query_adapter_callback` for `sql_table()`: adds `<cursor> <= now() -
    SAFETY_LAG` to the generated SELECT, leaving room for in-flight
    transactions on the cursor column to commit before their row is ever
    extracted. Pairs with `capped_incremental` -- see the SAFETY_LAG comment
    above for why this, and not `incremental(end_value=...)`, is the correct
    place to cap the window.
    """
    cursor_column = incremental.cursor_path
    cutoff = sa.func.now() - sa.text(f"interval '{int(SAFETY_LAG.total_seconds())} seconds'")
    return query.where(table.c[cursor_column] <= cutoff)


def bq_resource(resource: Any) -> Any:
    """bigquery_adapter(..., autodetect_schema=True): BigQuery's Parquet
    loader can't take an explicit JSON column type (only jsonl/avro can),
    and this hits both real JSONB columns (payments.risk,
    business_entities.fiscal_year) and Postgres ARRAY columns, which dlt
    also reflects as its abstract `json` type (business_entities.
    entity_characters). Setting the hint here, at the resource level, bakes
    it into the schema at extract time -- setting autodetect_schema only on
    the destination() config does NOT reliably apply to already
    normalized-but-not-loaded packages (verified 2026-07-11: a stale
    pending package from before the destination-level fix kept failing
    until the resource-level hint + a `drop-pending-packages` were both
    applied). Same constraint will hit the real pipeline once it lands
    these columns.
    """
    return bigquery_adapter(resource, autodetect_schema=True)


def psycopg_dsn(env: Env) -> str:
    """Raw psycopg connection string -- strip the `+psycopg` SQLAlchemy
    driver marker dlt's sql_table() credentials need but psycopg.connect()
    doesn't understand.
    """
    return env.pg_dsn.replace("postgresql+psycopg://", "postgresql://")


def bq_pipeline(pipeline_name: str, env: Env) -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name=pipeline_name,
        # location: BigQuery defaults to the "US" multi-region, which the
        # data-platform org policy (gcp.resourceLocations, restricted to
        # me-central2 for residency -- see docs/provisioning.md) correctly
        # rejects. Must match the region datasets/buckets were created in.
        destination=dlt.destinations.bigquery(project_id=env.gcp_project, location="me-central2"),
        staging=dlt.destinations.filesystem(bucket_url=f"gs://{env.gcs_bucket}/pg-test"),
        dataset_name="raw_test",
    )
