"""checkout_session database →
{BQ_DATASET_RAW}.checkout_session__checkout_sessions — one pipeline per
source database (a pipeline connects to exactly one DB), incremental append.

Mutable source, watermarked on `updated_at` with the safety-lag cap: every
update re-extracts the row, so raw holds one appended row per source-row
version and downstream dedups to the latest (see utils/dlt_helpers.py and
the README's watermark design).

`updated_at` is NULLABLE in the source schema, unlike payments — but the
service sets it on both insert paths and bumps it on every update
(checkout-session-service/src/app/repositories/checkout-session.repository.ts),
so NULLs are app-prevented, not schema-enforced. A row that ever landed
with NULL would be invisible to the watermark predicate forever; if a
source NULL-count check ever comes back nonzero, backfill manually rather
than trusting the cursor. One insert path stamps it from the app clock
(`new Date()`), not Postgres `now()` — the safety lag absorbs that skew
too. No index on `updated_at` at source (only created_at / merchant_id /
order_id / idempotency_key), so every run scans the table — same
deferred-index situation as settlement; revisit with LiteCore if the
table grows.

No column allowlist — ingest-everything posture. Known-sensitive columns
(trim map in docs/schema-management.md §1): `customer` JSONB is shopper
contact data; `metadata`/`order_data` are merchant-supplied JSONB and may
carry anything; `payment_url` is a live checkout capability URL until
`expires_on`.
"""

from __future__ import annotations

from dlt.sources.sql_database import sql_table

from app_etl.config import Settings
from app_etl.utils.dlt_helpers import (
    bq_pipeline,
    bq_resource,
    cap_upper_bound,
    capped_incremental,
    pg_credentials,
    refresh_mode,
)

DATABASE = "checkout_session"


def run() -> None:
    settings = Settings.from_env()

    checkout_sessions = bq_resource(
        sql_table(
            credentials=pg_credentials(settings, DATABASE),
            schema="public",
            table="checkout_sessions",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            table_name=f"{DATABASE}__checkout_sessions",
            primary_key="id",
            incremental=capped_incremental("updated_at"),
            write_disposition="append",
        ),
        partition="updated_at",
        cluster="merchant_id",
    )

    pipeline = bq_pipeline(DATABASE, settings)
    load_info = pipeline.run(
        checkout_sessions, loader_file_format="parquet", refresh=refresh_mode()
    )
    # per-table extracted row counts — load_info's summary doesn't include them
    print(pipeline.last_trace.last_normalize_info)
    print(load_info)
    load_info.raise_on_failed_jobs()


if __name__ == "__main__":
    run()
