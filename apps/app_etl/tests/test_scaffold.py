"""Scaffold sanity: every module imports and the settings contract holds."""

import pytest


def test_modules_import():
    import app_etl.config
    import app_etl.ingestion.ingest_merchants
    import app_etl.ingestion.ingest_payments
    import app_etl.utils.bq
    import app_etl.utils.gcs
    import app_etl.utils.pg
    import app_etl.utils.state  # noqa: F401


def test_settings_from_env(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT", "lite-data-dev")
    monkeypatch.setenv("GCS_BUCKET", "lite-data-raw-dev")
    monkeypatch.setenv("PG_DSN", "postgresql://u:p@localhost:5432/litecore")

    from app_etl.config import Settings

    s = Settings.from_env()
    assert s.gcp_project == "lite-data-dev"
    assert s.bq_dataset_raw == "raw_litecore"  # default
    assert s.bq_dataset_ops == "ops"  # default


def test_settings_requires_project(monkeypatch):
    monkeypatch.delenv("GCP_PROJECT", raising=False)
    monkeypatch.setenv("GCS_BUCKET", "b")
    monkeypatch.setenv("PG_DSN", "d")

    from app_etl.config import Settings

    with pytest.raises(KeyError):
        Settings.from_env()
