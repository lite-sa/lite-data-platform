"""Section II.2: inactive POS terminals.

Monthly. The Section II.1 population (terminals in service in the month,
including those pulled out in it, plus TIDs that took a payment) minus
the active terminals. ASSIGNED terminals are allocated to a merchant but
not yet activated on the device; they count as inactive.
"""

from app_etl.export.sama.common import run, show
from app_etl.export.sama.pi.common import TERMINAL_ACTIVITY

REPORT_MONTH = "2026-08"

DETAIL_SQL = TERMINAL_ACTIVITY

ANSWER_SQL = f"""
select
    countif(not is_active) as inactive_terminals,
    countif(not is_active and terminal_status = 'ASSIGNED') as of_which_assigned,
    countif(not is_active and terminal_type = 'SOFTPOS') as of_which_softpos,
    countif(not is_active and is_pulled_out) as of_which_pulled_out,
    count(*) as terminals
from ({DETAIL_SQL})
"""

if __name__ == "__main__":
    answer = run(ANSWER_SQL, REPORT_MONTH)
    detail = run(DETAIL_SQL, REPORT_MONTH)
    show(answer, detail, REPORT_MONTH)
