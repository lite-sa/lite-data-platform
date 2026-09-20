"""checkout_session database ->
{BQ_DATASET_RAW}.checkout_session__checkout_sessions.

Incremental append on `updated_at` with the safety-lag cap. `updated_at` is
nullable at source; the service always sets it, but a NULL row would never
be extracted.

Sensitive columns: `customer` (shopper contact data), `metadata` /
`order_data` (merchant-supplied), `payment_url` (live until `expires_on`).
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
