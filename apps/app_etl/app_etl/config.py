"""Environment-driven settings, one flat object.

Local dev loads a gitignored `.env` at the repo root (see `.env.example`);
shell variables win. Cloud Run jobs get the same variables as job env vars.

Ingestion jobs set exactly one Postgres connection mode; other jobs set
neither. The database name is never config: each pipeline file passes its
own to `pg_credentials()`.

1. `PG_HOST` (+ `PG_PORT`, `PG_USER`): Cloud SQL Auth Proxy on localhost
   with `--auto-iam-authn`. The laptop mode.
2. `PG_INSTANCE_CONNECTION_NAME` (+ `PG_IAM_USER`): in-process IAM auth via
   the Cloud SQL Python Connector. The Cloud Run mode.
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

    # The dataset dbt builds into. dbt reads it from the env in
    # profiles.yml; the notify job reads it here.
    bq_dataset_core: str = "core"

    # dlt staging bucket, e.g. lite-data-dev-raw. Ingestion jobs only.
    gcs_bucket: str | None = None

    # Report egress bucket, e.g. lite-data-dev-egress. Export jobs only.
    gcs_bucket_egress: str | None = None

    # Slack incoming-webhook URL, a secret. Notify job only.
    slack_webhook_url: str | None = None

    # Mode 1: Cloud SQL Auth Proxy on localhost, e.g. 127.0.0.1:5432
    pg_host: str | None = None
    pg_port: int = 5432
    pg_user: str | None = None      # IAM principal, e.g. m.adel@lite.sa
    # Mode 2: Cloud SQL IAM auth, e.g. lite-litecore-dev:me-central2:non-cde-postgres
    pg_instance: str | None = None
    pg_iam_user: str | None = None  # SA email *minus* ".gserviceaccount.com"
    # "private" (same VPC) | "public" | "psc" (Private Service Connect, for
    # an instance in another, unpeered VPC or project).
    pg_ip_type: str = "private"

    def pg_dsn(self, db: str) -> str:
        """Proxy-mode DSN for database `db`; url-encodes the '@' in the IAM
        user.
        """
        user = urllib.parse.quote(self.pg_user, safe="")
        return f"postgresql+psycopg://{user}@{self.pg_host}:{self.pg_port}/{db}"

    @classmethod
    def from_env(cls) -> Settings:
        # usecwd: search upwards from the working directory, not from this
        # module's install location.
        load_dotenv(find_dotenv(usecwd=True))
        if os.environ.get("PG_DSN"):
            raise ValueError(
                "PG_DSN is retired: set PG_HOST/PG_PORT/PG_USER (Cloud SQL Auth "
                "Proxy) instead — the database name now lives in each pipeline "
                "file, not in the env"
            )
        pg_host = os.environ.get("PG_HOST")
        pg_instance = os.environ.get("PG_INSTANCE_CONNECTION_NAME")
        # Neither set is fine here: pg_credentials() fails for the jobs that
        # need a mode.
        if pg_host and pg_instance:
            raise ValueError(
                "set at most one of PG_HOST or PG_INSTANCE_CONNECTION_NAME, got both"
            )
        ip_type = os.environ.get("PG_IP_TYPE", "private")
        if ip_type not in ("private", "public", "psc"):
            raise ValueError(f"PG_IP_TYPE must be 'private', 'public', or 'psc', got {ip_type!r}")
        return cls(
            gcp_project=os.environ["GCP_PROJECT"],
            bq_dataset_raw=os.environ.get("BQ_DATASET_RAW", "raw_litecore"),
            bq_dataset_core=os.environ.get("BQ_DATASET_CORE", "core"),
            gcs_bucket=os.environ.get("GCS_BUCKET"),
            gcs_bucket_egress=os.environ.get("GCS_BUCKET_EGRESS"),
            slack_webhook_url=os.environ.get("SLACK_WEBHOOK_URL"),
            pg_host=pg_host,
            pg_port=int(os.environ.get("PG_PORT", "5432")),
            pg_user=os.environ["PG_USER"] if pg_host else None,
            pg_instance=pg_instance,
            pg_iam_user=os.environ["PG_IAM_USER"] if pg_instance else None,
            pg_ip_type=ip_type,
        )
