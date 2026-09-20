"""ledger database -> {BQ_DATASET_RAW}.ledger__{account,entry}.

Both tables incremental append on `updated_at` with the safety-lag cap.
`account` is incremental, not a `replace` snapshot: its balance and status
change on every transaction. `entry` mutates in place through hold ->
capture -> release.

Polling sees only the state current at extraction time: versions between
two runs are lost.
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

DATABASE = "ledger"


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
        # `owner` is the merchant id when owner_type = MERCHANT.
        cluster="owner",
    )

    entry = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="entry",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            table_name=f"{DATABASE}__entry",
            primary_key="id",
            incremental=capped_incremental("updated_at"),
            write_disposition="append",
        ),
        partition="updated_at",
        cluster="account_id",
    )

    pipeline = bq_pipeline(DATABASE, settings)
    load_info = pipeline.run(
        [account, entry], loader_file_format="parquet", refresh=refresh_mode()
    )
    # per-table extracted row counts — load_info's summary doesn't include them
    print(pipeline.last_trace.last_normalize_info)
    print(load_info)
    load_info.raise_on_failed_jobs()


if __name__ == "__main__":
    run()
