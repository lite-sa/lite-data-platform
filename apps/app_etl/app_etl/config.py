"""Environment-driven settings — one flat object, no layering.

Local dev keeps a gitignored `.env` at the repo root (see `.env.example`),
loaded via python-dotenv; variables already set in the shell always win.
Cloud Run jobs get the same variables as job env vars — no `.env` there.

The Postgres source has exactly two connection modes (set one, never both).
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
    gcp_project: str      # GCP project id (single dev project for now)
    gcs_bucket: str       # raw landing bucket, e.g. lite-data-raw-dev
    bq_dataset_raw: str   # landing dataset in BQ, e.g. raw_litecore

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
        if bool(pg_host) == bool(pg_instance):
            raise ValueError(
                "set exactly one of PG_HOST or PG_INSTANCE_CONNECTION_NAME "
                f"(PG_HOST {'set' if pg_host else 'unset'}, "
                f"PG_INSTANCE_CONNECTION_NAME {'set' if pg_instance else 'unset'})"
            )
        ip_type = os.environ.get("PG_IP_TYPE", "private")
        if ip_type not in ("private", "public"):
            raise ValueError(f"PG_IP_TYPE must be 'private' or 'public', got {ip_type!r}")
        return cls(
            gcp_project=os.environ["GCP_PROJECT"],
            gcs_bucket=os.environ["GCS_BUCKET"],
            bq_dataset_raw=os.environ.get("BQ_DATASET_RAW", "raw_litecore"),
            pg_host=pg_host,
            pg_port=int(os.environ.get("PG_PORT", "5432")),
            pg_user=os.environ["PG_USER"] if pg_host else None,
            pg_instance=pg_instance,
            pg_iam_user=os.environ["PG_IAM_USER"] if pg_instance else None,
            pg_ip_type=ip_type,
        )
