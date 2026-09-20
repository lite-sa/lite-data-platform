"""payment_v2 database -> {BQ_DATASET_RAW}.payment_v2__{payments,
payment_operations,threeds,payment_link,payment_link_consumption}.

All tables incremental append on `updated_at` with the safety-lag cap: raw
holds one row per source-row version, downstream dedups to the latest.

Joins: `threeds.payment_id` -> payments.id, nullable (left-join from
threeds); `payment_link_consumption.link_id` -> payment_link.id.

Sensitive columns: threeds customer / device / order_data and the EMV 3DS
artifacts (authentication_value is the CAVV); payment_link customer /
metadata; payment_link_consumption ip_address / user_agent / session_id.
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
        sql_table(
            credentials=credentials,
            schema="public",
            table="payments",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            table_name=f"{DATABASE}__payments",
            primary_key="id",
            incremental=capped_incremental("updated_at"),
            write_disposition="append",
        ),
        partition="updated_at",
        cluster="merchant_id",
    )

    payment_operations = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="payment_operations",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            table_name=f"{DATABASE}__payment_operations",
            primary_key="id",
            incremental=capped_incremental("updated_at"),
            write_disposition="append",
        ),
        partition="updated_at",
        cluster="payment_id",
    )

    threeds = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="threeds",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            table_name=f"{DATABASE}__threeds",
            primary_key="id",
            incremental=capped_incremental("updated_at"),
            write_disposition="append",
        ),
        partition="updated_at",
        # The main access path is the join to payments.
        cluster="payment_id",
    )

    payment_link = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="payment_link",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            table_name=f"{DATABASE}__payment_link",
            primary_key="id",
            incremental=capped_incremental("updated_at"),
            write_disposition="append",
        ),
        partition="updated_at",
        cluster="merchant_id",
    )

    payment_link_consumption = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="payment_link_consumption",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            table_name=f"{DATABASE}__payment_link_consumption",
            primary_key="id",
            incremental=capped_incremental("updated_at"),
            write_disposition="append",
        ),
        partition="updated_at",
        cluster="link_id",
    )

    pipeline = bq_pipeline(DATABASE, settings)
    load_info = pipeline.run(
        [payments, payment_operations, threeds, payment_link, payment_link_consumption],
        loader_file_format="parquet",
        refresh=refresh_mode(),
    )
    # per-table extracted row counts — load_info's summary doesn't include them
    print(pipeline.last_trace.last_normalize_info)
    print(load_info)
    load_info.raise_on_failed_jobs()


if __name__ == "__main__":
    run()
