"""Unit tests for the config contract and the shared dlt helpers.

Only hand-written logic is under test: the two-mode connection contract,
the DSN quoting, and the hints baked into dlt resources. Missing-env-var
KeyErrors and argparse behavior are deliberately not re-tested.
"""

import dlt
import pytest
import sqlalchemy as sa

from app_etl.config import Settings
from app_etl.utils.dlt_helpers import bq_resource, pg_credentials


@pytest.fixture
def clean_env(tmp_path, monkeypatch):
    """Hermetic base env: no repo-root .env, no ambient PG_*/BQ_* exports."""
    monkeypatch.chdir(tmp_path)  # keep any repo-root .env out of find_dotenv()
    monkeypatch.setenv("GCP_PROJECT", "p")
    monkeypatch.setenv("GCS_BUCKET", "b")
    for var in (
        "BQ_DATASET_RAW",
        "PG_DSN",
        "PG_HOST",
        "PG_PORT",
        "PG_USER",
        "PG_INSTANCE_CONNECTION_NAME",
        "PG_IAM_USER",
        "PG_IP_TYPE",
    ):
        monkeypatch.delenv(var, raising=False)
    return monkeypatch


def test_pipeline_modules_import():
    # The only place the entry-point modules are imported before Cloud Run.
    import app_etl.ingestion.payment_v2  # noqa: F401


def test_settings_proxy_mode(clean_env):
    clean_env.setenv("PG_HOST", "127.0.0.1")
    clean_env.setenv("PG_USER", "m.adel@lite.sa")

    s = Settings.from_env()
    assert s.pg_instance is None
    assert s.pg_port == 5432  # default
    assert s.bq_dataset_raw == "raw_litecore"  # default


def test_settings_cloudsql_mode(clean_env):
    clean_env.setenv("PG_INSTANCE_CONNECTION_NAME", "proj:me-central2:inst")
    clean_env.setenv("PG_IAM_USER", "sa-app-etl@proj.iam")

    s = Settings.from_env()
    assert s.pg_host is None
    assert s.pg_instance == "proj:me-central2:inst"
    assert s.pg_ip_type == "private"  # default


def test_settings_rejects_both_pg_modes(clean_env):
    clean_env.setenv("PG_HOST", "127.0.0.1")
    clean_env.setenv("PG_USER", "u")
    clean_env.setenv("PG_INSTANCE_CONNECTION_NAME", "proj:me-central2:inst")

    with pytest.raises(ValueError, match="at most one"):
        Settings.from_env()


def test_settings_no_pg_mode_loads_but_cannot_connect(clean_env):
    """Transform/export jobs set no source connection at all — loading must
    succeed (they never touch Postgres) and the failure must move to the
    point of use, where an ingestion job would hit it at startup."""
    s = Settings.from_env()
    assert s.pg_host is None and s.pg_instance is None

    with pytest.raises(ValueError, match="no source connection"):
        pg_credentials(s, "payment_v2")


def test_pg_dsn_quotes_iam_email():
    """The '@' in an IAM username must be url-encoded, never hand-written."""
    s = Settings(
        gcp_project="p",
        gcs_bucket="b",
        bq_dataset_raw="r",
        pg_host="127.0.0.1",
        pg_port=5432,
        pg_user="m.adel@lite.sa",
    )
    assert s.pg_dsn("payment_v2") == (
        "postgresql+psycopg://m.adel%40lite.sa@127.0.0.1:5432/payment_v2"
    )


def test_pg_credentials_engine_is_lazy():
    """Instance mode must build its Engine without network or credentials —
    connections come lazily from the Cloud SQL connector (this is what lets
    CI construct pipelines with no GCP access)."""
    engine = pg_credentials(
        Settings(
            gcp_project="p",
            gcs_bucket="b",
            bq_dataset_raw="r",
            pg_instance="proj:me-central2:inst",
            pg_iam_user="sa-app-etl@proj.iam",
        ),
        "payment_v2",
    )
    assert isinstance(engine, sa.engine.Engine)
    assert engine.dialect.name == "postgresql"


def test_bq_resource_partition_hint():
    """The partition column must carry dlt's x-bigquery-partition prop — with
    autodetect_schema the BQ load job reads it to set DAY time-partitioning
    at table creation (create-time only; see refresh_mode for changes).
    """

    @dlt.resource(name="t")
    def rows():
        yield {"updated_at": "2026-01-01T00:00:00Z"}

    schema = bq_resource(rows(), partition="updated_at").compute_table_schema()
    assert schema["columns"]["updated_at"].get("x-bigquery-partition") is True
    assert schema.get("x-bigquery-autodetect-schema") is True
