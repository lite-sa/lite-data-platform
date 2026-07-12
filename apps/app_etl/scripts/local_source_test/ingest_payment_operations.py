"""Disposable smoke pipeline: payment_v2.payment_operations -> raw_test.payment_operations."""

from __future__ import annotations

from dlt.sources.sql_database import sql_table

from _common import bq_pipeline, bq_resource, cap_upper_bound, capped_incremental, load_env


def run() -> None:
    env = load_env()

    table = bq_resource(
        sql_table(
            credentials=env.pg_dsn,
            schema="payment_v2",
            table="payment_operations",
            query_adapter_callback=cap_upper_bound,
        ).apply_hints(
            incremental=capped_incremental("created_at"),
            write_disposition="append",
        )
    )

    pipeline = bq_pipeline("pg_source_smoke_test_payment_operations", env)
    load_info = pipeline.run(table, loader_file_format="parquet")
    print(load_info)
    load_info.raise_on_failed_jobs()


if __name__ == "__main__":
    run()
