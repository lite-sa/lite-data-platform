"""Environment-driven settings — one flat object, no layering.

Local dev exports these in the shell (or a .env you source yourself);
Cloud Run jobs get them as env vars. Moving to a prod project later is a
config change, not a code change.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    gcp_project: str      # GCP project id (single dev project for now)
    gcs_bucket: str       # raw landing bucket, e.g. lite-data-raw-dev
    bq_dataset_raw: str   # landing dataset in BQ, e.g. raw_litecore
    bq_dataset_ops: str   # operational metadata (ingestion_runs), e.g. ops
    pg_dsn: str           # read-replica DSN, e.g. postgresql://user:pass@host:5432/litecore

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            gcp_project=os.environ["GCP_PROJECT"],
            gcs_bucket=os.environ["GCS_BUCKET"],
            bq_dataset_raw=os.environ.get("BQ_DATASET_RAW", "raw_litecore"),
            bq_dataset_ops=os.environ.get("BQ_DATASET_OPS", "ops"),
            pg_dsn=os.environ["PG_DSN"],
        )
