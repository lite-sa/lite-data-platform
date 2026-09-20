"""payout database -> {BQ_DATASET_RAW}.payout__{transfer,transfer_transaction,
beneficiary,beneficiary_transaction,topup,topup_transaction}.

All tables incremental append on `updated_at` with the safety-lag cap; the
`*_transaction` audit tables are updated after insert too.

Joins: `transfer.instruction_settlement_id` -> settlement.instruction.id;
`topup.checkout_session_id` -> checkout_session.checkout_sessions.id;
`topup.wallet_id` -> ledger.account.id. `transfer.status` depends on
`transfer.direction`. Amounts are minor units; `fees` lands as a JSON string.

Skipped: `blocked_iban_attempt`, `cooldown_mechanism`, `otp_tokens`,
`databasechangelog*`.
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

DATABASE = "payout"


def run() -> None:
    settings = Settings.from_env()
    credentials = pg_credentials(settings, DATABASE)

    transfer = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="transfer",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            table_name=f"{DATABASE}__transfer",
            primary_key="id",
            incremental=capped_incremental("updated_at"),
            write_disposition="append",
        ),
        partition="updated_at",
        cluster="merchant_id",
    )

    transfer_transaction = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="transfer_transaction",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            table_name=f"{DATABASE}__transfer_transaction",
            primary_key="id",
            incremental=capped_incremental("updated_at"),
            write_disposition="append",
        ),
        partition="updated_at",
        # parent-FK convention, like payment_operations -> payment_id
        cluster="transfer_id",
    )

    beneficiary = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="beneficiary",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            table_name=f"{DATABASE}__beneficiary",
            primary_key="id",
            incremental=capped_incremental("updated_at"),
            write_disposition="append",
        ),
        partition="updated_at",
        cluster="merchant_id",
    )

    beneficiary_transaction = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="beneficiary_transaction",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            table_name=f"{DATABASE}__beneficiary_transaction",
            primary_key="id",
            incremental=capped_incremental("updated_at"),
            write_disposition="append",
        ),
        partition="updated_at",
        cluster="beneficiary_id",
    )

    topup = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="topup",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            table_name=f"{DATABASE}__topup",
            primary_key="id",
            incremental=capped_incremental("updated_at"),
            write_disposition="append",
        ),
        partition="updated_at",
        cluster="merchant_id",
    )

    topup_transaction = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="topup_transaction",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            table_name=f"{DATABASE}__topup_transaction",
            primary_key="id",
            incremental=capped_incremental("updated_at"),
            write_disposition="append",
        ),
        partition="updated_at",
        cluster="topup_id",
    )

    pipeline = bq_pipeline(DATABASE, settings)
    load_info = pipeline.run(
        [
            transfer,
            transfer_transaction,
            beneficiary,
            beneficiary_transaction,
            topup,
            topup_transaction,
        ],
        loader_file_format="parquet",
        refresh=refresh_mode(),
    )
    # per-table extracted row counts — load_info's summary doesn't include them
    print(pipeline.last_trace.last_normalize_info)
    print(load_info)
    load_info.raise_on_failed_jobs()


if __name__ == "__main__":
    run()
