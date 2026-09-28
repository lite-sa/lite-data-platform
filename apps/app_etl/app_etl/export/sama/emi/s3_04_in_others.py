"""Section III.4: other incoming wallet transactions, count and value.

Monthly. Every wallet credit created in the report month that moved money
(emi.common.IS_MOVEMENT) and is not an incoming bank transfer (that is
III.1): today mostly INTERNAL_SETTLEMENT, acquiring proceeds settled into
the merchant's wallet. Top-ups (III.2) post no known entry type yet; once
one completes, exclude its type here. For INTERNAL_SETTLEMENT rows,
external_reference_id is the settlement instruction id; the instruction
and its window are joined for cross-checking.
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
    e.external_reference_id,
    i.settlement_window_id as window_id,
    i.status as instruction_status,
    w.account_id,
    w.merchant_id,
    w.merchant_name,
    w.is_test,
    e.amount as amount_minor,
    e.amount / 100 as amount_sar,
    e.created_at
from {lv("ledger__entry")} as e
join {WALLETS} as w using (account_id)
left join {lv("settlement__instruction")} as i
    on i.id = e.external_reference_id and e.type = 'INTERNAL_SETTLEMENT'
where e.direction = 'CREDIT'
  and e.type != 'INCOMING_TRANSFER'
  and date(e.created_at, @tz) between @month_start and @month_end
  and {IS_MOVEMENT}
order by e.created_at
"""

ANSWER_SQL = f"""
select
    count(*) as volume,
    round(coalesce(sum(amount_minor), 0) / 100, 2) as value_sar,
    string_agg(distinct type, ', ') as entry_types
from ({DETAIL_SQL})
where not is_test
"""

if __name__ == "__main__":
    answer = run(ANSWER_SQL, REPORT_MONTH)
    detail = run(DETAIL_SQL, REPORT_MONTH)
    show(answer, detail, REPORT_MONTH)
