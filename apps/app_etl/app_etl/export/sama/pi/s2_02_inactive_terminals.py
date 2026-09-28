"""Section II.2: inactive POS terminals.

Monthly. Terminals still installed at month end (not removed by then)
with no authorized POS payment in the report month, from the same rows
as Section II.1. ASSIGNED terminals are allocated to a merchant but not
yet activated on the device; they count as inactive.
"""

from app_etl.export.sama.common import run, show
from app_etl.export.sama.pi.common import TERMINAL_ACTIVITY

REPORT_MONTH = "2026-08"

DETAIL_SQL = TERMINAL_ACTIVITY

ANSWER_SQL = f"""
select
    countif(is_installed and not is_active) as inactive_terminals,
    countif(is_installed and not is_active and terminal_status = 'ASSIGNED')
        as of_which_assigned,
    countif(is_installed and not is_active and terminal_type = 'SOFTPOS')
        as of_which_softpos,
    countif(is_installed) as installed_at_month_end
from ({DETAIL_SQL})
where not is_test
"""

if __name__ == "__main__":
    answer = run(ANSWER_SQL, REPORT_MONTH)
    detail = run(DETAIL_SQL, REPORT_MONTH)
    show(answer, detail, REPORT_MONTH)
