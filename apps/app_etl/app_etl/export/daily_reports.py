"""One Cloud Run job, every daily export report.

Runs each registered report in sequence with per-report isolation: a
finance failure never blocks merchant files (and vice versa), but any
failure still fails the job so the scheduler run shows red. Adding a
report = one entry in REPORTS; the job command stays
`python -m app_etl.export.daily_reports` forever, so no new Cloud Run
job, scheduler, or image plumbing per report — that manual overhead is
exactly what this module exists to cap.

    python -m app_etl.export.daily_reports                    # all reports
    python -m app_etl.export.daily_reports --report finance   # one report
    python -m app_etl.export.daily_reports --report finance --date 2026-08-10

--date / --dry-run are passed through to every selected report.
"""

from __future__ import annotations

import argparse
import traceback

from app_etl.export import settlement_daily_report


def _all_merchants(argv: list[str]) -> None:
    """settlement_daily_report in --all-merchants mode: one combined
    platform-wide CSV (test merchants included) to the finance folder.
    """
    settlement_daily_report.main([*argv, "--all-merchants"])


# Both entries are settlement_daily_report, once per mode: the delivered
# per-merchant files and the finance director's combined file.
REPORTS = {
    "merchant": settlement_daily_report.main,
    "finance": _all_merchants,
}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--report",
        action="append",
        choices=sorted(REPORTS),
        default=None,
        help="run only this report (repeatable); default: all",
    )
    parser.add_argument("--date", default=None, help="report day, passed through")
    parser.add_argument(
        "--dry-run", action="store_true", help="passed through to every report"
    )
    args = parser.parse_args(argv)

    passthrough = ["--date", args.date] if args.date else []
    if args.dry_run:
        passthrough.append("--dry-run")

    failures: list[str] = []
    for name in args.report or sorted(REPORTS):
        print(f"=== {name} ===")
        try:
            REPORTS[name](passthrough)
        except SystemExit as exc:  # a report's own blocking checks
            if exc.code not in (0, None):
                failures.append(f"{name}: {exc.code}")
        except Exception:
            traceback.print_exc()
            failures.append(f"{name}: unhandled exception (see traceback)")

    if failures:
        raise SystemExit("report failures:\n- " + "\n- ".join(failures))


if __name__ == "__main__":
    main()
