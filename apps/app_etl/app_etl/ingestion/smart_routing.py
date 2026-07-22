"""smart_routing database → {BQ_DATASET_RAW}.{profile,routing_rule,
transaction_evaluation} — one pipeline per source database (a pipeline
connects to exactly one DB).

`profile` and `routing_rule` are small mutable routing-config tables: full
extract every run, `replace` disposition — same interim pattern as
`merchants`/`business_entities` (snapshot_date partitioning is the target
design, not yet implemented). `transaction_evaluation` is the per-
transaction routing-decision log: mutable, timestamped, watermarked on
`updated_at` with the safety-lag cap, append disposition — same shape as
`payments`/`payment_operations` (see utils/dlt_helpers.py and the README's
watermark design).
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

DATABASE = "smart_routing"

# No PII on either routing-config table — every column is included.
PROFILE_COLUMNS = [
    "id",
    "merchant_id",
    "name",
    "status",
    "is_default",
    "is_deleted",
    "description",
    "created_at",
    "updated_at",
]

ROUTING_RULE_COLUMNS = [
    "id",
    "name",
    "type",
    "rule_definition",
    "profile_id",
    "priority",
    "scope",
    "is_deleted",
    "created_at",
    "updated_at",
]

# `transaction_initiated_message` excluded: it reads as the raw inbound
# event payload that kicked off the evaluation, same PII-risk shape as
# payments.customer/device/threeds_* and payment_operations.raw_provider_*.
# Everything else here is routing-engine output (matched_rules,
# routing_obj) or identifiers, not source request data.
TRANSACTION_EVALUATION_COLUMNS = [
    "id",
    "main_transaction_evaluation_id",
    "transaction_id",
    "payment_id",
    "operation_type",
    "matched_rules",
    "routing_obj",
    "gateway_id",
    "status",
    "published_event",
    "created_at",
    "updated_at",
    "merchant_id",
]


def run() -> None:
    settings = Settings.from_env()
    credentials = pg_credentials(settings, DATABASE)

    profile = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="profile",
            included_columns=PROFILE_COLUMNS,
        ).apply_hints(
            # TODO: add snapshot_date partitions for point-in-time joins
            write_disposition="replace"
        ),
        cluster="merchant_id",
    )

    routing_rule = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="routing_rule",
            included_columns=ROUTING_RULE_COLUMNS,
        ).apply_hints(
            # TODO: add snapshot_date partitions for point-in-time joins
            write_disposition="replace"
        ),
        cluster="profile_id",
    )

    transaction_evaluation = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="transaction_evaluation",
            included_columns=TRANSACTION_EVALUATION_COLUMNS,
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
        [profile, routing_rule, transaction_evaluation],
        loader_file_format="parquet",
        refresh=refresh_mode(),
    )
    print(load_info)
    load_info.raise_on_failed_jobs()


if __name__ == "__main__":
    run()
