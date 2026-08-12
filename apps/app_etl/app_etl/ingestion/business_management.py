"""business_management database →
{BQ_DATASET_RAW}.business_management__business_entities — full replace.

One pipeline per source database; `business_entities` is the only table we
take from `business_management` today. Small mutable config table: full
extract every run, `replace` disposition. (The snapshot_date-partitioned
design in the README is the intended end state; `replace` is the interim
until that strategy lands.)

No column allowlist — ingest-everything posture
"""

from __future__ import annotations

from dlt.sources.sql_database import sql_table

from app_etl.config import Settings
from app_etl.utils.dlt_helpers import bq_pipeline, bq_resource, pg_credentials, refresh_mode

DATABASE = "business_management"


def run() -> None:
    settings = Settings.from_env()

    business_entities = bq_resource(
        sql_table(
            credentials=pg_credentials(settings, DATABASE),
            schema="public",
            table="business_entities",
        ).apply_hints(
            # TODO: add snapshot_date partitions for point-in-time joins
            table_name=f"{DATABASE}__business_entities",
            write_disposition="replace",
        ),
        # `business_id` (not `id`, the source PK) is the natural join key:
        # it's the only other uniquely-indexed column and reads as this
        # row's external identifier, whereas `id` is only targeted by this
        # DB's own child tables (channels, channel_sequence) — confirm
        # against how `merchants`/`payments.merchant_id` key before relying
        # on it in a downstream join.
        cluster="business_id",
    )

    pipeline = bq_pipeline(DATABASE, settings)
    load_info = pipeline.run(
        business_entities, loader_file_format="parquet", refresh=refresh_mode()
    )
    # per-table extracted row counts — load_info's summary doesn't include them
    print(pipeline.last_trace.last_normalize_info)
    print(load_info)
    load_info.raise_on_failed_jobs()


if __name__ == "__main__":
    run()
