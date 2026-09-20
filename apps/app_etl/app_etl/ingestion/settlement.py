"""settlement database -> {BQ_DATASET_RAW}.settlement__{account,
settlement_window,instruction,transaction}.

All tables incremental append on `updated_at` with the safety-lag cap.
`account` is incremental, not a `replace` snapshot: reporting needs the
history of its `status` and `is_deleted` changes.

Joins: `transaction.parent_payment_id` -> payments.id;
`transaction.external_reference_id` -> payment_operations.id.
`transaction.fees` is jsonb[] and lands as a STRING holding a JSON array.

Skipped: `cycle`, `account_cycle` (schedule config), `databasechangelog*`.
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

DATABASE = "settlement"


def run() -> None:
    settings = Settings.from_env()
    credentials = pg_credentials(settings, DATABASE)

    account = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="account",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            table_name=f"{DATABASE}__account",
            primary_key="id",
            incremental=capped_incremental("updated_at"),
            write_disposition="append",
        ),
        partition="updated_at",
        cluster="merchant_id",
    )

    settlement_window = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="settlement_window",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            table_name=f"{DATABASE}__settlement_window",
            primary_key="id",
            incremental=capped_incremental("updated_at"),
            write_disposition="append",
        ),
        partition="updated_at",
        cluster="merchant_id",
    )

    instruction = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="instruction",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            table_name=f"{DATABASE}__instruction",
            primary_key="id",
            incremental=capped_incremental("updated_at"),
            write_disposition="append",
        ),
        partition="updated_at",
        # Cluster on the parent FK; merchant_id is on settlement_window.
        cluster="settlement_window_id",
    )

    transaction = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="transaction",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            table_name=f"{DATABASE}__transaction",
            primary_key="id",
            incremental=capped_incremental("updated_at"),
            write_disposition="append",
        ),
        partition="updated_at",
        cluster="settlement_window_id",
    )

    pipeline = bq_pipeline(DATABASE, settings)
    load_info = pipeline.run(
        [account, settlement_window, instruction, transaction],
        loader_file_format="parquet",
        refresh=refresh_mode(),
    )
    # per-table extracted row counts — load_info's summary doesn't include them
    print(pipeline.last_trace.last_normalize_info)
    print(load_info)
    load_info.raise_on_failed_jobs()


if __name__ == "__main__":
    run()
