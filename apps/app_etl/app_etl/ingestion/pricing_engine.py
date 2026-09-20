"""pricing_engine database -> {BQ_DATASET_RAW}.pricing_engine__rule_evaluation.

Incremental append on `updated_at` with the safety-lag cap. One row per
pricing request, written in two phases (insert, then update with the
result).

Joins: for `origin = payment-v2-service`, `origin_reference` ->
payment_operations.id and `parent_origin_reference` -> payments.id;
payout-service references match no table. A reference can be evaluated more
than once: readers take the latest per reference. `winning_rule_id` NULL
means no rule matched. JSON columns land as strings; amounts are minor units.

Skipped: the four profile config tables, `databasechangelog*`.
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

DATABASE = "pricing_engine"


def run() -> None:
    settings = Settings.from_env()

    rule_evaluation = bq_resource(
        sql_table(
            credentials=pg_credentials(settings, DATABASE),
            schema="public",
            table="rule_evaluation",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            table_name=f"{DATABASE}__rule_evaluation",
            primary_key="id",
            incremental=capped_incremental("updated_at"),
            write_disposition="append",
        ),
        partition="updated_at",
        # Joins reach this table on origin_reference = operation id.
        cluster="origin_reference",
    )

    pipeline = bq_pipeline(DATABASE, settings)
    load_info = pipeline.run(
        rule_evaluation, loader_file_format="parquet", refresh=refresh_mode()
    )
    # per-table extracted row counts — load_info's summary doesn't include them
    print(pipeline.last_trace.last_normalize_info)
    print(load_info)
    load_info.raise_on_failed_jobs()


if __name__ == "__main__":
    run()
