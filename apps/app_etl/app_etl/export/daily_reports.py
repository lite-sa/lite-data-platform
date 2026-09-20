"""One Cloud Run job for every daily export report.

Runs each report in REPORTS in sequence. One report's failure never blocks
the others, but any failure fails the job. Adding a report is one entry in
REPORTS.

    python -m app_etl.export.daily_reports                    # all reports
    python -m app_etl.export.daily_reports --report finance --date 2026-08-10

--date / --dry-run pass through to every selected report.
"""

from __future__ import annotations

import argparse
import traceback

from app_etl.export import settlement_daily_report


def _all_merchants(argv: list[str]) -> None:
    """settlement_daily_report in --all-merchants mode."""
    settlement_daily_report.main([*argv, "--all-merchants"])


# settlement_daily_report once per mode: per-merchant files, then the
# combined finance file.
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
