"""Disposable smoke pipeline: user.merchants -> raw_test.merchants."""

from __future__ import annotations

from dlt.sources.sql_database import sql_table

from _common import bq_pipeline, bq_resource, load_env


def run() -> None:
    env = load_env()

    table = bq_resource(
        sql_table(credentials=env.pg_dsn, schema="user", table="merchants").apply_hints(
            write_disposition="replace"
        )
    )

    pipeline = bq_pipeline("pg_source_smoke_test_merchants", env)
    load_info = pipeline.run(table, loader_file_format="parquet")
    print(load_info)
    load_info.raise_on_failed_jobs()


if __name__ == "__main__":
    run()
