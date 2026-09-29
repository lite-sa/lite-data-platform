"""Section IV.3: other outgoing wallet transactions, count and value.

Monthly. Every wallet debit created in the report month that moved money
(emi.common.IS_MOVEMENT) and is not a payout (those are IV.1 and IV.2): today
refunds and payment reversals funded from the wallet. Card spend counts once
captured (CAPTURE_ISSUING, DEBIT_ISSUING); an authorization still open at
month end is in Section I.1's open-holds memo, not here.
"""

from app_etl.export.sama.common import lv, run, show
from app_etl.export.sama.emi.common import IS_MOVEMENT, WALLETS

REPORT_MONTH = "2026-08"

DETAIL_SQL = f"""
select
    e.id as entry_id,
    e.type,
    e.method,
    e.parent_transaction_id,
    w.account_id,
    w.merchant_id,
    w.merchant_name,
    w.is_test,
    e.amount as amount_minor,
    e.amount / 100 as amount_sar,
    e.created_at
from {lv("ledger__entry")} as e
join {WALLETS} as w using (account_id)
where e.direction = 'DEBIT'
  and e.type != 'CAPTURE_BENEFICIARY_TRANSFER'
  and date(e.created_at, @tz) between @month_start and @month_end
  and {IS_MOVEMENT}
order by e.created_at
"""

ANSWER_SQL = f"""
select
    count(*) as volume,
    round(coalesce(sum(amount_minor), 0) / 100, 2) as value_sar
from ({DETAIL_SQL})
"""

if __name__ == "__main__":
    answer = run(ANSWER_SQL, REPORT_MONTH)
    detail = run(DETAIL_SQL, REPORT_MONTH)
    show(answer, detail, REPORT_MONTH)
