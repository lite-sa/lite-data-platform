"""Environment-driven settings — one flat object, no layering.

Same env-var contract as app_etl (single dev GCP project for now). dlt
gets what it needs (GCS staging bucket, BigQuery dataset, PG connection)
translated from these at each pipeline's call site — no dlt.toml, no
dlt-managed secrets file, one source of truth for config across both
apps.

Postgres connection is split into PG_HOST/PORT/USER/DATABASE rather than
one PG_DSN string, so switching databases (this instance hosts one per
service: payment_v2, risk_management, ledger, ...) is a one-line env
change instead of rebuilding a DSN. No password field: this assumes
Cloud SQL IAM database auth through a locally running Cloud SQL Auth
Proxy (`cloud-sql-proxy --auto-iam-authn`) — the same passwordless
mechanism LiteCore's own services use in cloud. PG_USER is your IAM
identity (e.g. your @lite.sa email); the proxy handles the actual auth
handshake, so nothing here ever holds a DB password.
"""

from __future__ import annotations

import os
import urllib.parse
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    gcp_project: str      # GCP project id (single dev project for now)
    gcs_bucket: str        # dlt filesystem staging root: "bucket" or "bucket/prefix"
                           # if you don't have create-bucket access, point this at a
                           # folder in an existing bucket, e.g. "old-bucket/lite-data-platform"
    bq_dataset_raw: str    # landing dataset in BQ, e.g. raw_litecore
    bq_location: str        # must match the GCS bucket's region — BigQuery load jobs
                            # require the dataset and source bucket to be co-located
    pg_host: str           # e.g. 127.0.0.1 (Cloud SQL Auth Proxy, run separately)
    pg_port: int           # e.g. 5432
    pg_user: str           # IAM identity authorized on the instance, e.g. you@lite.sa
    pg_database: str       # e.g. payment_v2, risk_management — the one thing you'll change often

    @property
    def pg_dsn(self) -> str:
        """SQLAlchemy-style DSN for dlt's sql_table(credentials=...). Builds
        it from the parts above so callers never hand-encode the '@' in an
        IAM email themselves."""
        user = urllib.parse.quote(self.pg_user, safe="")
        return f"postgresql+psycopg://{user}@{self.pg_host}:{self.pg_port}/{self.pg_database}"

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            gcp_project=os.environ["GCP_PROJECT"],
            gcs_bucket=os.environ["GCS_BUCKET"],
            bq_dataset_raw=os.environ.get("BQ_DATASET_RAW", "raw_litecore"),
            bq_location=os.environ.get("BQ_LOCATION", "me-central2"),
            pg_host=os.environ.get("PG_HOST", "127.0.0.1"),
            pg_port=int(os.environ.get("PG_PORT", "5432")),
            pg_user=os.environ["PG_USER"],
            pg_database=os.environ["PG_DATABASE"],
        )
