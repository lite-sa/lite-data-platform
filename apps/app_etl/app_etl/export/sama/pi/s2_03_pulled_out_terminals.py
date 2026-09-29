"""Section II.3: POS terminals pulled out.

Monthly. Terminals removed in the report month: the inventory sets
deleted_at (status TERMINATED) when a terminal is taken back. A terminal
that took payments before its removal in the same month also counts as
active in Section II.1.
"""

from app_etl.export.sama.common import run, show
from app_etl.export.sama.pi.common import TERMINAL_ACTIVITY

REPORT_MONTH = "2026-08"

DETAIL_SQL = TERMINAL_ACTIVITY

ANSWER_SQL = f"""
select
    countif(is_pulled_out) as pulled_out_terminals,
    countif(is_pulled_out and is_active) as of_which_active_in_month
from ({DETAIL_SQL})
"""

if __name__ == "__main__":
    answer = run(ANSWER_SQL, REPORT_MONTH)
    detail = run(DETAIL_SQL, REPORT_MONTH)
    show(answer, detail, REPORT_MONTH)
