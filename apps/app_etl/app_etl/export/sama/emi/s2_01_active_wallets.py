"""Section II.1: number of active wallets.

Monthly. A wallet is active when it has at least one money movement
(emi.common.IS_MOVEMENT: holds, released holds, the accounting credit legs
and compartment moves left out) created in the report month. Population:
emi.common.WALLETS.
"""

from app_etl.export.sama.common import run, show
from app_etl.export.sama.emi.common import WALLET_ACTIVITY

REPORT_MONTH = "2026-08"

DETAIL_SQL = WALLET_ACTIVITY

ANSWER_SQL = f"""
select
    countif(is_active) as active_wallets,
    count(*) as wallets
from ({DETAIL_SQL})
"""

if __name__ == "__main__":
    answer = run(ANSWER_SQL, REPORT_MONTH)
    detail = run(DETAIL_SQL, REPORT_MONTH)
    show(answer, detail, REPORT_MONTH)
