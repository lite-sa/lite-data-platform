"""settlement database → {BQ_DATASET_RAW}.settlement__{account,
settlement_window,instruction,transaction} — one pipeline per source
database (a pipeline connects to exactly one DB), all four tables
incremental append.

Mutable sources, watermarked on `updated_at` with the safety-lag cap:
every update re-extracts the row, so raw holds one appended row per
source-row version and downstream dedups to the latest (see
utils/dlt_helpers.py and the README's watermark design). `account` reads
like a per-merchant dimension but gets the incremental shape, not a
`replace` snapshot: `status` transitions and the `is_deleted` soft-delete
flip are exactly what payout reporting needs history for, and `replace`
would erase them — same call as ledger's `account`.

The other two service tables, `cycle` and `account_cycle`, are
settlement-schedule config — deliberately skipped until reporting needs
the cycle dimension. The FK columns pointing at them
(`settlement_window.cycle_id`, `instruction.account_cycle_id`) land with
their tables, so the join works whenever they're added.
`databasechangelog*` is Liquibase's own bookkeeping, never ingested.

No column allowlists — ingest-everything posture 

Join contract for `transaction`, verified on dev data: `parent_payment_id` ->
payments.id, `external_reference_id` -> payment_operations.id (the
CAPTURE/AUTHORIZE that produced the leg) — NOT payments.id as first
presumed. `fees` is jsonb[], the first array-of-jsonb through the
loader; it lands in BQ as a STRING holding a JSON array (mdr/vat legs),
so staging parses it; settled_amount ≈ amount − Σfees on dev data.
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
        # parent-FK convention, like payment_operations -> payment_id;
        # merchant_id lives one join away on settlement_window
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
