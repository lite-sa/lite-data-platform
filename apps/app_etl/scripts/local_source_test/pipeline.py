"""Disposable smoke test: 4 real LiteCore table shapes, local Postgres ->
real GCS -> real BigQuery.

This is NOT the real pipeline (see app_etl/ingestion/ -- step 2 is still
blocked on the ops/watermark decision in docs/schema-management.md). It only
proves the dlt sql_database -> filesystem(GCS) -> bigquery wiring against
the actual lite-data-dev resources, using local Docker Postgres (seeded from
the real LiteCore migrations) as a stand-in source until a real replica
connection exists.

Writes to a `raw_test` BQ dataset and a `pg-test/` GCS prefix -- never
`raw_litecore`. Teardown: see README.md in this folder.

Each table has its own pipeline in its own ingest_<table>.py file (own
dlt.pipeline_name, own state) -- this script just runs them all in sequence,
mirroring the shape the real per-table ingestion files will eventually take,
just without the independent Cloud Scheduler cadences.
"""

from __future__ import annotations

import ingest_business_entities
import ingest_merchants
import ingest_payment_operations
import ingest_payments


def run() -> None:
    ingest_payments.run()
    ingest_payment_operations.run()
    ingest_merchants.run()
    ingest_business_entities.run()


if __name__ == "__main__":
    run()
