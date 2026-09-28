"""Section II.1: active POS terminals.

Monthly. A terminal is active when its TID took at least one authorized
POS payment in the report month. Covers POS and SOFTPOS terminal types.
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
    countif(is_active and payment_merchant_id != merchant_id) as merchant_moved
from ({DETAIL_SQL})
where not is_test
"""

if __name__ == "__main__":
    answer = run(ANSWER_SQL, REPORT_MONTH)
    detail = run(DETAIL_SQL, REPORT_MONTH)
    show(answer, detail, REPORT_MONTH)
