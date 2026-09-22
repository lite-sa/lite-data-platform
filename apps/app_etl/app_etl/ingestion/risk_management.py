"""risk_management database -> {BQ_DATASET_RAW}.risk_management__{risk_rule,
transaction_rule_evaluation,transaction_rule_decision,velocity_bucket,
velocity_event}.

`risk_rule` is incremental append on `updated_at`: reporting needs the
history of `is_enabled`/`is_deleted` changes, not just the latest rule
state. `transaction_rule_evaluation` and `transaction_rule_decision` have no
`updated_at` (insert-once event tables), so they're incremental on their own
timestamp: `evaluation_time` and `decided_at`. `velocity_bucket` and
`velocity_event` are incremental append on `updated_at`: buckets accumulate
counts through the hour and events can be updated after insert (e.g.
`status`, `captured_amount`).

Joins: `transaction_rule_evaluation.rule_id` / `transaction_rule_group_mapping`
-> risk_rule.id; `transaction_rule_evaluation.transaction_id` and
`transaction_rule_decision.transaction_id` -> payments.id. A transaction can
be evaluated more than once (re-review): readers take the latest per
transaction_id by `evaluation_time`/`decided_at`. `velocity_event.transaction_id`
-> payments.id; `velocity_bucket`/`velocity_event` have no direct FK to
risk_rule, they're read by the velocity rule type via `param_type`/
`param_value`. `rule_definition` (risk_rule) lands as a STRING holding a
JSON object.

Skipped: `risk_rule_group`, `risk_rule_group_member`, `risk_rule_group_mapping`
(rule-group config, not needed for reporting yet), `databasechangelog*`.
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


def run() -> None:
    settings = Settings.from_env()
    credentials = pg_credentials(settings, DATABASE)

    risk_rule = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="risk_rule",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            table_name=f"{DATABASE}__risk_rule",
            primary_key="id",
            incremental=capped_incremental("updated_at"),
            write_disposition="append",
        ),
        partition="updated_at",
        cluster="merchant_id",
    )

    transaction_rule_evaluation = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="transaction_rule_evaluation",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            table_name=f"{DATABASE}__transaction_rule_evaluation",
            primary_key="id",
            incremental=capped_incremental("evaluation_time"),
            write_disposition="append",
        ),
        partition="evaluation_time",
        cluster="transaction_id",
    )

    transaction_rule_decision = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="transaction_rule_decision",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            table_name=f"{DATABASE}__transaction_rule_decision",
            primary_key="id",
            incremental=capped_incremental("decided_at"),
            write_disposition="append",
        ),
        partition="decided_at",
        cluster="transaction_id",
    )

    velocity_bucket = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="velocity_bucket",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            table_name=f"{DATABASE}__velocity_bucket",
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
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            table_name=f"{DATABASE}__velocity_event",
            primary_key="id",
            incremental=capped_incremental("updated_at"),
            write_disposition="append",
        ),
        partition="updated_at",
        cluster="merchant_id",
    )

    pipeline = bq_pipeline(DATABASE, settings)
    load_info = pipeline.run(
        [
            risk_rule,
            transaction_rule_evaluation,
            transaction_rule_decision,
            velocity_bucket,
            velocity_event,
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
