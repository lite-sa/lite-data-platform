"""Environment-driven settings — one flat object, no layering.

Local dev keeps a gitignored `.env` at the repo root (see `.env.example`),
loaded via python-dotenv; variables already set in the shell always win.
Cloud Run jobs get the same variables as job env vars — no `.env` there.

The Postgres source has exactly two connection modes (set one, never both;
non-ingestion jobs — transform — set neither and never touch Postgres, so
`pg_credentials()` is where "no mode at all" fails, not here).
Both are per-*instance*: the database name is NOT config — ingestion is one
pipeline per source database, so each pipeline file states its own database
and passes it to `pg_dsn()` / `pg_credentials()`.

1. `PG_HOST` (+ `PG_PORT`, `PG_USER`) — the Cloud SQL Auth Proxy listening
   on localhost (`--auto-iam-authn` mode: IAM username only, no password —
   the proxy injects the token). This is the laptop mode.
2. `PG_INSTANCE_CONNECTION_NAME` (+ `PG_IAM_USER`) — in-process Cloud SQL
   IAM auth via the Python Connector; no password, no secret to rotate.
   This is the Cloud Run mode. See `utils/dlt_helpers.py` `pg_credentials()`.
"""

from __future__ import annotations

import os
import urllib.parse
from dataclasses import dataclass

from dotenv import find_dotenv, load_dotenv


@dataclass(frozen=True)
class Settings:
    gcp_project: str            # GCP project id (single dev project for now)
    bq_dataset_raw: str         # landing dataset in BQ, e.g. raw_litecore
    bq_dataset_aml: str = "aml" # dbt marts dataset (aml_merchant_features)

    # Raw landing bucket (dlt staging), e.g. lite-data-dev-raw. Required by
    # ingestion only — bq_pipeline() enforces it; the transform job leaves
    # it unset.
    gcs_bucket: str | None = None

    # Mode 1: Cloud SQL Auth Proxy on localhost, e.g. 127.0.0.1:5432
    pg_host: str | None = None
    pg_port: int = 5432
    pg_user: str | None = None      # IAM principal, e.g. m.adel@lite.sa
    # Mode 2: Cloud SQL IAM auth, e.g. lite-litecore-dev:me-central2:non-cde-postgres
    pg_instance: str | None = None
    pg_iam_user: str | None = None  # SA email *minus* ".gserviceaccount.com"
    pg_ip_type: str = "private"     # "private" (Cloud Run) | "public" (laptop)

    def pg_dsn(self, db: str) -> str:
        """SQLAlchemy-style DSN for dlt's sql_table(credentials=...). Builds
        it from the parts above so callers never hand-encode the '@' in an
        IAM email themselves. `db` comes from the pipeline file — one
        pipeline per source database.
        """
        user = urllib.parse.quote(self.pg_user, safe="")
        return f"postgresql+psycopg://{user}@{self.pg_host}:{self.pg_port}/{db}"

    @classmethod
    def from_env(cls) -> Settings:
        # usecwd: search from the working directory upwards (we run from the
        # repo root), not from this installed module's location.
        load_dotenv(find_dotenv(usecwd=True))
        if os.environ.get("PG_DSN"):
            raise ValueError(
                "PG_DSN is retired: set PG_HOST/PG_PORT/PG_USER (Cloud SQL Auth "
                "Proxy) instead — the database name now lives in each pipeline "
                "file, not in the env"
            )
        pg_host = os.environ.get("PG_HOST")
        pg_instance = os.environ.get("PG_INSTANCE_CONNECTION_NAME")
        # Both set is always a contradiction; neither set is fine — jobs
        # that never touch Postgres (transform) run without a mode, and
        # pg_credentials() fails loudly for the ones that need it.
        if pg_host and pg_instance:
            raise ValueError(
                "set at most one of PG_HOST or PG_INSTANCE_CONNECTION_NAME, got both"
            )
        ip_type = os.environ.get("PG_IP_TYPE", "private")
        if ip_type not in ("private", "public"):
            raise ValueError(f"PG_IP_TYPE must be 'private' or 'public', got {ip_type!r}")
        return cls(
            gcp_project=os.environ["GCP_PROJECT"],
            bq_dataset_raw=os.environ.get("BQ_DATASET_RAW", "raw_litecore"),
            bq_dataset_aml=os.environ.get("BQ_DATASET_AML", "aml"),
            gcs_bucket=os.environ.get("GCS_BUCKET"),
            pg_host=pg_host,
            pg_port=int(os.environ.get("PG_PORT", "5432")),
            pg_user=os.environ["PG_USER"] if pg_host else None,
            pg_instance=pg_instance,
            pg_iam_user=os.environ["PG_IAM_USER"] if pg_instance else None,
            pg_ip_type=ip_type,
        )
