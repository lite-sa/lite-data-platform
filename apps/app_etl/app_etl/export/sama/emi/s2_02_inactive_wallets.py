"""Section II.2: number of inactive wallets.

Monthly. The wallet population at month end minus the active wallets of
Section II.1, from the same per-wallet rows.
"""

from app_etl.export.sama.common import run, show
from app_etl.export.sama.emi.common import WALLET_ACTIVITY

REPORT_MONTH = "2026-08"

DETAIL_SQL = WALLET_ACTIVITY

ANSWER_SQL = f"""
select
    countif(not is_active) as inactive_wallets,
    count(*) as wallets,
    countif(is_active) as active_wallets
from ({DETAIL_SQL})
"""

if __name__ == "__main__":
    answer = run(ANSWER_SQL, REPORT_MONTH)
    detail = run(DETAIL_SQL, REPORT_MONTH)
    show(answer, detail, REPORT_MONTH)
