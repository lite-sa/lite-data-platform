"""Section III.1: incoming bank transfers into wallets, count and value.

Monthly. INCOMING_TRANSFER credit entries on a WALLET account created in
the report month. The payout transfer behind each entry
(parent_transaction_id) is joined for cross-checking.
"""

from app_etl.export.sama.common import lv, run, show
from app_etl.export.sama.emi.common import WALLETS

REPORT_MONTH = "2026-08"

DETAIL_SQL = f"""
select
    e.id as entry_id,
    e.parent_transaction_id as transfer_id,
    w.account_id,
    w.merchant_id,
    w.merchant_name,
    w.is_test,
    e.amount as amount_minor,
    e.amount / 100 as amount_sar,
    e.created_at,
    t.direction as transfer_direction,
    t.status as transfer_status
from {lv("ledger__entry")} as e
join {WALLETS} as w using (account_id)
left join {lv("payout__transfer")} as t on t.id = e.parent_transaction_id
where e.type = 'INCOMING_TRANSFER'
  and e.direction = 'CREDIT'
  and date(e.created_at, @tz) between @month_start and @month_end
order by e.created_at
"""

ANSWER_SQL = f"""
select
    count(*) as volume,
    round(coalesce(sum(amount_minor), 0) / 100, 2) as value_sar
from ({DETAIL_SQL})
where not is_test
"""

if __name__ == "__main__":
    answer = run(ANSWER_SQL, REPORT_MONTH)
    detail = run(DETAIL_SQL, REPORT_MONTH)
    show(answer, detail, REPORT_MONTH)
