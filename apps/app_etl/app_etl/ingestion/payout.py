"""payout database → {BQ_DATASET_RAW}.payout__{transfer,transfer_transaction,
beneficiary,beneficiary_transaction,topup,topup_transaction} — one pipeline
per source database, all six tables incremental append on `updated_at`
with the safety-lag cap (see utils/dlt_helpers.py). Three header/audit
pairs, the payments + payment_operations pattern; the audit tables are not
insert-only (rows get updated after creation), hence the same cursor.

Source facts that matter downstream:

- `transfer.direction` splits OUTGOING payouts from INCOMING wallet credits
  parsed from ANB statements (ALM-638), and `status` is polymorphic on it
  (`completed` / `manual_review` exist on both sides).
- `transfer.instruction_settlement_id` = settlement.instruction.id, set by
  the execute-settlement workflow.
- The ledger posts `external_reference_id` = transfer_transaction.id (the
  audit row, not the header) on hold / capture / release — verify on prod.
- `topup.checkout_session_id` -> checkout_session.checkout_sessions.id,
  `topup.wallet_id` -> ledger.account.id.
- Amounts are minor units; `fees` is a jsonb array landed as a JSON string
  and `total_amount` = `amount` + Σ fees. No `updated_at` index at source.

Skipped: `blocked_iban_attempt` (risk signal, no consumer yet),
`cooldown_mechanism` (rows are hard-deleted past `lock_until`),
`otp_tokens` (secrets + contact info), `databasechangelog*`.

No column allowlist: ingest-everything posture; the trim map is in
docs/schema-management.md §1 and the mirror models apply it.
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
