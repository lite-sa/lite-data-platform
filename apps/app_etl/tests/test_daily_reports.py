"""Unit tests for the daily_reports runner (registry plumbing only, the
reports themselves are tested in their own suites) and the raw freshness
guard every report calls.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

import app_etl.export.daily_reports as runner
from app_etl.export import common


def test_passthrough_reaches_the_selected_report(monkeypatch):
    calls: dict[str, list[str]] = {}
    monkeypatch.setitem(
        runner.REPORTS, "merchant", lambda argv: calls.setdefault("merchant", argv)
    )
    runner.main(["--report", "merchant", "--date", "2026-08-19", "--dry-run"])
    assert calls == {"merchant": ["--date", "2026-08-19", "--dry-run"]}


def test_failure_is_isolated_but_still_fails_the_job(monkeypatch):
    ran: list[str] = []

    def blocked(argv):
        ran.append("finance")
        raise SystemExit("report-blocking check failures")

    monkeypatch.setitem(runner.REPORTS, "finance", blocked)
    monkeypatch.setitem(runner.REPORTS, "merchant", lambda argv: ran.append("merchant"))
    with pytest.raises(SystemExit) as exc:
        runner.main([])
    # Every report ran despite the first one blocking, and the job is red.
    assert ran == ["finance", "merchant"]
    assert "finance" in str(exc.value)


def test_finance_appends_the_scope_flag(monkeypatch):
    seen: dict[str, list[str]] = {}
    monkeypatch.setattr(
        runner.settlement_daily_report,
        "main",
        lambda argv: seen.setdefault("argv", argv),
    )
    runner.REPORTS["finance"](["--date", "2026-08-19", "--dry-run"])
    assert seen["argv"] == ["--date", "2026-08-19", "--dry-run", "--all-merchants"]


class _LoadsClient:
    """Stands in for bigquery.Client: returns the covered schema names."""

    def __init__(self, covered):
        self.covered = covered
        self.cutoff = None

    def query_and_wait(self, query, job_config):
        params = {p.name: p for p in job_config.query_parameters}
        self.cutoff = params["cutoff"].value
        return [{"schema_name": name} for name in self.covered]


def test_guard_passes_when_every_required_database_loaded():
    client = _LoadsClient(["payment_v2", "settlement"])
    common.require_fresh_extraction(client, "p.raw", date(2026, 9, 20))
    # The Riyadh day 09-20 closes at 21:00 UTC.
    assert client.cutoff == datetime(2026, 9, 20, 21, 0, tzinfo=timezone.utc)


def test_guard_refuses_and_names_the_missing_database():
    client = _LoadsClient(["payment_v2"])
    with pytest.raises(SystemExit) as exc:
        common.require_fresh_extraction(client, "p.raw", date(2026, 9, 20))
    assert "settlement" in str(exc.value)
    assert "payment_v2" not in str(exc.value)
