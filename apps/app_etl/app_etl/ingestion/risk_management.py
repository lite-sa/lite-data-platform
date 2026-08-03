"""risk_management database → {BQ_DATASET_RAW}.{risk_rule,
transaction_rule_evaluation,velocity_bucket,velocity_event} — one pipeline
per source database (a pipeline connects to exactly one DB).

Two shapes here:

- `risk_rule`: small mutable rule-config table, no natural append-only
  log — full replace, same interim pattern as `merchants`/
  `business_entities` (snapshot_date partitioning is the target design,
  not yet implemented).
- `transaction_rule_evaluation`: true append-only evaluation log — no
  `updated_at` at all, rows never change after insert. Watermarked on its
  own timestamp (`evaluation_time`) with the safety-lag cap, same as any
  other incremental pipeline (see utils/dlt_helpers.py and the README's
  watermark design) — just the simplest case, since there's no mutation to
  worry about.
- `velocity_bucket`, `velocity_event`: mutable aggregate/event rows —
  confirmed empirically (a meaningful fraction of rows have
  `updated_at != created_at`: buckets keep incrementing as new transactions
  land in them, events fill in `captured_amount` later). Same shape as
  `ledger.account`: watermarked on `updated_at`, `append` disposition — raw
  holds one appended row per source-row version, downstream dedups to the
  latest.
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

DATABASE = "risk_management"

# No PII on the rule-definition columns: rule_definition is analyst-authored
# rule logic (fact/path/operator references like "customer.email", not raw
# customer data), and windows is just a duration list (e.g. "1h"). merchant_id
# is a business identifier, not PII.
RISK_RULE_COLUMNS = [
    "id",
    "name",
    "description",
    "status",
    "level",
    "score",
    "is_test",
    "is_enabled",
    "type",
    "windows",
    "is_deleted",
    "scope",
    "merchant_id",
    "rule_definition",
    "created_at",
    "updated_at",
]

TRANSACTION_RULE_EVALUATION_COLUMNS = [
    "id",
    "transaction_id",
    "rule_id",
    "result",
    "score_impact",
    "evaluation_time",
    "is_test",
]

# `param_value` excluded: confirmed against real data (not just "pending a
# look" — actually checked), it holds raw customer PII when
# param_type='email'/'phone' (e.g. an actual email address or phone number),
# and non-PII (amount/currency/instrumentId/paymentMethod) values for other
# param_types. It's the same column carrying both depending on a runtime
# value, so a column-level allowlist can't split it — excluding entirely
# until there's a deliberate, scoped way to include only the non-PII
# param_types. `param_type` itself (the category label) is fine.
VELOCITY_BUCKET_COLUMNS = [
    "id",
    "param_type",
    "bucket_hour",
    "merchant_id",
    "tx_count",
    "tx_auth_count",
    "tx_declined_count",
    "amount_sum",
    "amount_sum_auth",
    "amount_sum_declined",
    "amount_sum_captured",
    "created_at",
    "updated_at",
]

VELOCITY_EVENT_COLUMNS = [
    "id",
    "param_type",
    "merchant_id",
    "amount",
    "currency",
    "transaction_id",
    "status",
    "occurred_at",
    "created_at",
    "captured_amount",
    "updated_at",
]


def run() -> None:
    settings = Settings.from_env()
    credentials = pg_credentials(settings, DATABASE)

    risk_rule = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="risk_rule",
            included_columns=RISK_RULE_COLUMNS,
        ).apply_hints(
            # TODO: add snapshot_date partitions for point-in-time joins
            write_disposition="replace"
        ),
        cluster="merchant_id",
    )

    transaction_rule_evaluation = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="transaction_rule_evaluation",
            included_columns=TRANSACTION_RULE_EVALUATION_COLUMNS,
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            primary_key="id",
            incremental=capped_incremental("evaluation_time"),
            write_disposition="append",
        ),
        partition="evaluation_time",
        cluster="transaction_id",
    )

    velocity_bucket = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="velocity_bucket",
            included_columns=VELOCITY_BUCKET_COLUMNS,
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            primary_key="id",
            incremental=capped_incremental("updated_at"),
            write_disposition="append",
        ),
        partition="updated_at",
        cluster="merchant_id",
    )

    velocity_event = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="velocity_event",
            included_columns=VELOCITY_EVENT_COLUMNS,
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            primary_key="id",
            incremental=capped_incremental("updated_at"),
            write_disposition="append",
        ),
        partition="updated_at",
        cluster="merchant_id",
    )

    pipeline = bq_pipeline(DATABASE, settings)
    load_info = pipeline.run(
        [risk_rule, transaction_rule_evaluation, velocity_bucket, velocity_event],
        loader_file_format="parquet",
        refresh=refresh_mode(),
    )
    print(load_info)
    load_info.raise_on_failed_jobs()


if __name__ == "__main__":
    run()
