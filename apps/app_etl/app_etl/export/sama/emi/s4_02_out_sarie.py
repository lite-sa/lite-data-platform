"""Section IV.2: outgoing wallet transfers on SARIE, count and value.

Monthly. Wallet payouts booked in the report month (emi.common.WALLET_PAYOUTS)
whose rail is SARIE: the ANB channel says SARIE, or the channel is ANB or
missing and the amount is at or above emi.common.IPS_LIMIT_MINOR.
Check columns: rows classified by the amount rule, and rows found in only
one of ledger and payout (should be 0).
"""

from app_etl.export.sama.common import run, show
from app_etl.export.sama.emi.common import WALLET_PAYOUTS

REPORT_MONTH = "2026-08"

DETAIL_SQL = WALLET_PAYOUTS

ANSWER_SQL = f"""
select
    count(*) as volume,
    round(coalesce(sum(amount_minor), 0) / 100, 2) as value_sar,
    countif(rail_basis = 'amount') as by_amount_rule,
    countif(matched != 'both') as unmatched
from ({DETAIL_SQL})
where not is_test and rail = 'SARIE'
"""

if __name__ == "__main__":
    answer = run(ANSWER_SQL, REPORT_MONTH)
    detail = run(DETAIL_SQL, REPORT_MONTH)
    show(answer, detail, REPORT_MONTH)
