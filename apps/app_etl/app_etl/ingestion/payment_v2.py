"""payment_v2 database → {BQ_DATASET_RAW}.{payments,payment_operations} —
one pipeline per source database (a pipeline connects to exactly one DB),
both tables incremental append.

Mutable sources, watermarked on `updated_at` with the safety-lag cap: every
update re-extracts the row, so raw holds one appended row per source-row
version and downstream dedups to the latest (see utils/dlt_helpers.py and
the README's watermark design). Each resource keeps its own cursor inside
this pipeline's state.
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

DATABASE = "payment_v2"


def run() -> None:
    settings = Settings.from_env()
    credentials = pg_credentials(settings, DATABASE)

    payments = bq_resource(
        # TODO: add the column allowlist before pointing at a non-dummy source;
        # jsonb columns to deny first: risk, customer, order_data, device,
        # threeds_input, threeds_result, return_url, metadata, routing_result,
        # risk_result
        sql_table(
            credentials=credentials,
            schema="public",
            table="payments",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            primary_key="id",
            incremental=capped_incremental("updated_at"),
            write_disposition="append",
        ),
        partition="updated_at",
        cluster="merchant_id",
    )

    payment_operations = bq_resource(
        # TODO: add the column allowlist before pointing at a non-dummy source
        sql_table(
            credentials=credentials,
            schema="public",
            table="payment_operations",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            primary_key="id",
            incremental=capped_incremental("updated_at"),
            write_disposition="append",
        ),
        partition="updated_at",
        cluster="payment_id",
    )

    pipeline = bq_pipeline(DATABASE, settings)
    load_info = pipeline.run(
        [payments, payment_operations], loader_file_format="parquet", refresh=refresh_mode()
    )
    print(load_info)
    load_info.raise_on_failed_jobs()


if __name__ == "__main__":
    run()
