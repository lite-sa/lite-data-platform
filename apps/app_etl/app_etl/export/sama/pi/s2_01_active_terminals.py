"""Section II.1: active POS terminals.

Monthly. Population: terminals created by month end and not removed
before it started (pi.common.TERMINALS), plus any TID that took a POS
payment in the month. Active: the TID took at least one authorized POS
payment in the report month. Covers POS and SOFTPOS terminal types.
Check columns: active TIDs missing from the inventory, and TIDs whose
payment merchant differs from the inventory's current merchant.
"""

from app_etl.export.sama.common import run, show
from app_etl.export.sama.pi.common import TERMINAL_ACTIVITY

REPORT_MONTH = "2026-08"

DETAIL_SQL = TERMINAL_ACTIVITY

ANSWER_SQL = f"""
select
    countif(is_active) as active_terminals,
    countif(is_active and terminal_type = 'SOFTPOS') as of_which_softpos,
    countif(is_active and not in_inventory) as not_in_inventory,
    countif(is_active and payment_merchant_id != merchant_id) as merchant_moved,
    count(*) as terminals
from ({DETAIL_SQL})
"""

if __name__ == "__main__":
    answer = run(ANSWER_SQL, REPORT_MONTH)
    detail = run(DETAIL_SQL, REPORT_MONTH)
    show(answer, detail, REPORT_MONTH)
