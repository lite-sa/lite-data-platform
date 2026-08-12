"""smart_routing database → {BQ_DATASET_RAW}.smart_routing__{profile,
routing_rule,transaction_evaluation} — one pipeline per source database
(a pipeline connects to exactly one DB).

`profile` and `routing_rule` are small mutable routing-config tables: full
extract every run, `replace` disposition — same interim pattern as
`merchants`/`business_entities` (snapshot_date partitioning is the target
design, not yet implemented). `transaction_evaluation` is the per-
transaction routing-decision log: mutable, timestamped, watermarked on
`updated_at` with the safety-lag cap, append disposition — same shape as
`payments`/`payment_operations` (see utils/dlt_helpers.py and the README's
watermark design).

No column allowlists — ingest-everything posture
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


def run() -> None:
    settings = Settings.from_env()
    credentials = pg_credentials(settings, DATABASE)

    profile = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="profile",
        ).apply_hints(
            # TODO: add snapshot_date partitions for point-in-time joins
            table_name=f"{DATABASE}__profile",
            write_disposition="replace",
        ),
        cluster="merchant_id",
    )

    routing_rule = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="routing_rule",
        ).apply_hints(
            # TODO: add snapshot_date partitions for point-in-time joins
            table_name=f"{DATABASE}__routing_rule",
            write_disposition="replace",
        ),
        cluster="profile_id",
    )

    transaction_evaluation = bq_resource(
        sql_table(
            credentials=credentials,
            schema="public",
            table="transaction_evaluation",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            table_name=f"{DATABASE}__transaction_evaluation",
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
    # per-table extracted row counts — load_info's summary doesn't include them
    print(pipeline.last_trace.last_normalize_info)
    print(load_info)
    load_info.raise_on_failed_jobs()


if __name__ == "__main__":
    run()
