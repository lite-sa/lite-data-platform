"""sample-ingest smoke test: settings load and the run logs successfully."""


def test_main_logs_settings(monkeypatch, caplog):
    monkeypatch.setenv("GCP_PROJECT", "lite-data-dev")
    monkeypatch.setenv("GCS_BUCKET", "lite-data-raw-dev")
    monkeypatch.setenv("PG_DSN", "postgresql://u:p@localhost:5432/litecore")

    from app_etl.sample_ingest import main

    with caplog.at_level("INFO", logger="sample_ingest"):
        main()

    assert "sample-ingest ok" in caplog.text
    assert "lite-data-dev" in caplog.text
