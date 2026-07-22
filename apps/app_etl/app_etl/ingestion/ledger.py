"""ledger database → {BQ_DATASET_RAW}.{account,entry} — one pipeline per
source database (a pipeline connects to exactly one DB), both tables
incremental append.

Mutable sources, watermarked on `updated_at` with the safety-lag cap: every
update re-extracts the row, so raw holds one appended row per source-row
version and downstream dedups to the latest (see utils/dlt_helpers.py and
the README's watermark design). `account`'s balance/status mutate on
every transaction — treating it as a `replace` snapshot like
`business_entities` would throw away the balance trajectory that risk/
exposure analysis needs, so it gets the same incremental shape as `entry`
despite reading like a per-entity dimension table. `entry` itself mutates
in place through its hold -> capture -> release lifecycle (hold_at /
captured_at / released_at columns on the same row), same pattern as
`payments`.

Caveat inherent to polling any Postgres table, not specific to this
pipeline: if a row's `updated_at` advances more than once between two
ingestion runs, only whichever state was current at extraction time is
ever visible — Postgres `UPDATE` overwrites the tuple in place, so
intermediate states aren't queryable once superseded, regardless of
watermark design. Capturing every transition losslessly needs WAL-based
CDC (e.g. Datastream), not polling; out of scope for v1.
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

# `account_number`/`virtual_iban` excluded: real bank-account identifiers
# (an IBAN enables direct SEPA payment initiation, arguably more sensitive
# than a masked card number). `opened_by` excluded: reads as an internal
# staff identifier, same pattern as agent_email/sales_email in
# business_entities/payment_operations. Everything else is account
# metadata/balance, not personal data.
ACCOUNT_COLUMNS = [
    "id",
    "name",
    "direction",
    "status",
    "tag",
    "type",
    "owner_type",
    "owner_product_type",
    "owner",
    "currency",
    "available_balance",
    "description",
    "last_transaction_at",
    "created_at",
    "updated_at",
]

# No PII on this table — every column is included. Note: `order_id` here
# is a smallint intra-transaction sequence number, unrelated to
# payments.order_id despite the shared name.
ENTRY_COLUMNS = [
    "id",
    "source",
    "method",
    "external_reference_id",
    "type",
    "amount",
    "direction",
    "currency",
    "exchange_rate",
    "available_balance_after",
    "account_id",
    "parent_transaction_id",
    "created_at",
    "updated_at",
    "hold_at",
    "captured_at",
    "released_at",
    "order_id",
]


def run() -> None:
    settings = Settings.from_env()
    credentials = pg_credentials(settings, DATABASE)

    account = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="account",
            included_columns=ACCOUNT_COLUMNS,
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            primary_key="id",
            incremental=capped_incremental("updated_at"),
            write_disposition="append",
        ),
        partition="updated_at",
        # `owner` is the platform-wide join key when owner_type=MERCHANT
        # (unconfirmed against real data) — matches the merchant_id/
        # business_id clustering convention on the other pipelines.
        cluster="owner",
    )

    entry = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="entry",
            included_columns=ENTRY_COLUMNS,
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
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
    print(load_info)
    load_info.raise_on_failed_jobs()


if __name__ == "__main__":
    run()
