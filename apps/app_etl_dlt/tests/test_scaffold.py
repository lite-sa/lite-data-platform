"""Scaffold sanity: every module imports, and the settings/table-registry
contracts hold."""

from datetime import datetime, timezone

import pytest


def test_modules_import():
    import app_etl_dlt.config
    import app_etl_dlt.runner
    import app_etl_dlt.tables  # noqa: F401


def test_settings_from_env(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT", "lite-data-dev")
    monkeypatch.setenv("GCS_BUCKET", "lite-data-raw-dev")
    monkeypatch.setenv("PG_USER", "u@lite.sa")
    monkeypatch.setenv("PG_DATABASE", "litecore")

    from app_etl_dlt.config import Settings

    s = Settings.from_env()
    assert s.gcp_project == "lite-data-dev"
    assert s.bq_dataset_raw == "raw_litecore"  # default
    assert s.bq_location == "me-central2"  # default
    assert s.pg_host == "127.0.0.1"  # default
    assert s.pg_port == 5432  # default


def test_settings_requires_project(monkeypatch):
    monkeypatch.delenv("GCP_PROJECT", raising=False)
    monkeypatch.setenv("GCS_BUCKET", "b")
    monkeypatch.setenv("PG_USER", "u@lite.sa")
    monkeypatch.setenv("PG_DATABASE", "d")

    from app_etl_dlt.config import Settings

    with pytest.raises(KeyError):
        Settings.from_env()


def test_pg_dsn_url_encodes_at_sign_in_user(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT", "lite-data-dev")
    monkeypatch.setenv("GCS_BUCKET", "b")
    monkeypatch.setenv("PG_USER", "a.farooq@lite.sa")
    monkeypatch.setenv("PG_DATABASE", "payment_v2")

    from app_etl_dlt.config import Settings

    s = Settings.from_env()
    assert s.pg_dsn == "postgresql+psycopg://a.farooq%40lite.sa@127.0.0.1:5432/payment_v2"


def test_table_config_requires_cursor_for_incremental():
    from app_etl_dlt.tables import TableConfig

    with pytest.raises(ValueError):
        TableConfig(name="bad", mode="incremental", cadence="hourly")


def test_table_config_snapshot_needs_no_cursor():
    from app_etl_dlt.tables import TableConfig

    # Should not raise: snapshot mode has no incremental fields to validate.
    TableConfig(name="fine", mode="snapshot", cadence="daily")


def test_run_table_rejects_unknown_name():
    from app_etl_dlt.runner import run_table

    with pytest.raises(ValueError, match="unknown table"):
        run_table("does_not_exist")


def test_table_config_accepts_incremental_fields():
    from app_etl_dlt.tables import TableConfig

    t = TableConfig(
        name="payments",
        mode="incremental",
        cadence="hourly",
        cursor_column="created_at",
        initial_value=datetime(2025, 1, 1, tzinfo=timezone.utc),
        primary_key="id",
    )
    assert t.cursor_column == "created_at"
