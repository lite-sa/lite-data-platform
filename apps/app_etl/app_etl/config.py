"""Environment-driven settings — one flat object, no layering.

Local dev keeps a gitignored `.env` at the repo root (see `.env.example`),
loaded via python-dotenv; variables already set in the shell always win.
Cloud Run jobs get the same variables as job env vars — no `.env` there.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import find_dotenv, load_dotenv


@dataclass(frozen=True)
class Settings:
    gcp_project: str      # GCP project id (single dev project for now)
    gcs_bucket: str       # raw landing bucket, e.g. lite-data-raw-dev
    bq_dataset_raw: str   # landing dataset in BQ, e.g. raw_litecore
    bq_dataset_ops: str   # operational metadata (ingestion_runs), e.g. ops
    pg_dsn: str           # read-replica DSN, e.g. postgresql+psycopg://user:pass@host:5432/litecore

    @classmethod
    def from_env(cls) -> Settings:
        # usecwd: search from the working directory upwards (we run from the
        # repo root), not from this installed module's location.
        load_dotenv(find_dotenv(usecwd=True))
        return cls(
            gcp_project=os.environ["GCP_PROJECT"],
            gcs_bucket=os.environ["GCS_BUCKET"],
            bq_dataset_raw=os.environ.get("BQ_DATASET_RAW", "raw_litecore"),
            bq_dataset_ops=os.environ.get("BQ_DATASET_OPS", "ops"),
            pg_dsn=os.environ["PG_DSN"],
        )
