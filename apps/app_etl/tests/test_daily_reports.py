"""Unit tests for the daily_reports runner — registry plumbing only, the
reports themselves are tested in their own suites.
"""

from __future__ import annotations

import pytest

import app_etl.export.daily_reports as runner


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
