"""payment_v2 database → {BQ_DATASET_RAW}.payment_v2__{payments,
payment_operations,threeds,payment_link,payment_link_consumption} — one
pipeline per source database (a pipeline connects to exactly one DB),
all tables incremental append.

Mutable sources, watermarked on `updated_at` with the safety-lag cap: every
update re-extracts the row, so raw holds one appended row per source-row
version and downstream dedups to the latest (see utils/dlt_helpers.py and
the README's watermark design). Each resource keeps its own cursor inside
this pipeline's state.

Join contract for the 2026-08-17 additions: `threeds.payment_id` →
payments.id but nullable — an authentication can exist before/without its
payment, so left-join from threeds, not inner;
`payment_link_consumption.link_id` → payment_link.id (FK, enforced at
source). `payment_link.id` is a uuid (first uuid PK through this
pipeline; lands as STRING like the varchar(36) ids).

No column allowlists — ingest-everything posture. Known-sensitive columns
(trim map in docs/schema-management.md §1): threeds carries
customer/device/order_data JSONB and the EMV 3DS artifacts —
authentication_value is the CAVV cryptogram; payment_link carries
customer/metadata; payment_link_consumption carries
ip_address/user_agent/session_id.
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
        # payment_id, not merchant_id: the primary access path is the join
        # to payments (3DS outcome per payment) — same call as
        # payment_operations. Nullable is fine for BQ clustering.
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
