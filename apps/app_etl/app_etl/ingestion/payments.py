"""payment_v2.payments → {BQ_DATASET_RAW}.payments — incremental append.

Append-only source, watermarked on `created_at` with the safety-lag cap
(see utils/dlt_helpers.py and the README's watermark design).
"""

from __future__ import annotations

from dlt.sources.sql_database import sql_table

from app_etl.config import Settings
from app_etl.utils.dlt_helpers import (
    bq_pipeline,
    bq_resource,
    cap_upper_bound,
    capped_incremental,
)


def run() -> None:
    settings = Settings.from_env()

    table = bq_resource(
        sql_table(
            credentials=settings.pg_dsn,
            schema="payment_v2",
            table="payments",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            incremental=capped_incremental("created_at"),
            write_disposition="append",
        )
    )

    pipeline = bq_pipeline("payments", settings)
    load_info = pipeline.run(table, loader_file_format="parquet")
    print(load_info)
    load_info.raise_on_failed_jobs()


if __name__ == "__main__":
    run()
