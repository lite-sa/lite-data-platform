"""user.merchants → {BQ_DATASET_RAW}.merchants — full replace.

Small mutable config table: full extract every run, `replace` disposition.
(The snapshot_date-partitioned design in the README is the intended end
state; `replace` is the interim until that strategy lands.)
"""

from __future__ import annotations

from dlt.sources.sql_database import sql_table

from app_etl.config import Settings
from app_etl.utils.dlt_helpers import bq_pipeline, bq_resource


def run() -> None:
    settings = Settings.from_env()

    table = bq_resource(
        # TODO: add the column allowlist
        sql_table(credentials=settings.pg_dsn, schema="user", table="merchants").apply_hints(
            # TODO: add snapshot_date  partitions for point-in-time joins
            write_disposition="replace"
        )
    )

    pipeline = bq_pipeline("merchants", settings)
    load_info = pipeline.run(table, loader_file_format="parquet")
    print(load_info)
    load_info.raise_on_failed_jobs()


if __name__ == "__main__":
    run()
