"""Run all 4 real ingestion pipelines in sequence against the local Docker
Postgres stand-in (see README.md; the pipelines themselves live in
app_etl/ingestion/). Keep BQ_DATASET_RAW=raw_test in the repo-root .env so
nothing here ever writes to raw_litecore.
"""

from __future__ import annotations

from app_etl.ingestion import business_entities, merchants, payment_operations, payments


def run() -> None:
    payments.run()
    payment_operations.run()
    merchants.run()
    business_entities.run()


if __name__ == "__main__":
    run()
