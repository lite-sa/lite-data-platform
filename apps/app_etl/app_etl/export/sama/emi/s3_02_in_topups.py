"""Section III.2: wallet top-ups, count and value.

Monthly. Filed as zero until the product launches: no top-up has
completed yet. The query counts completed payout top-ups created in the
report month, so it reports real numbers once they exist; the ledger
entry type a completed top-up posts is still unknown.
"""

from app_etl.export.sama.common import lv, not_deleted, run, show
from app_etl.export.sama.emi.common import WALLETS

REPORT_MONTH = "2026-08"

DETAIL_SQL = f"""
select
    t.id as topup_id,
    t.wallet_id,
    t.merchant_id,
    w.merchant_name,
    coalesce(w.is_test, t.merchant_id in unnest(@test_ids), false) as is_test,
    t.status,
    t.amount as amount_minor,
    t.amount / 100 as amount_sar,
    t.created_at,
    t.updated_at
from {lv("payout__topup")} as t
left join {WALLETS} as w on w.account_id = t.wallet_id
where date(t.created_at, @tz) between @month_start and @month_end
  and {not_deleted("t.merchant_id")}
order by t.created_at
"""

ANSWER_SQL = f"""
select
    countif(status = 'completed') as volume,
    round(coalesce(sum(if(status = 'completed', amount_minor, 0)), 0) / 100, 2)
        as value_sar,
    count(*) as topups_created
from ({DETAIL_SQL})
"""

if __name__ == "__main__":
    answer = run(ANSWER_SQL, REPORT_MONTH)
    detail = run(DETAIL_SQL, REPORT_MONTH)
    show(answer, detail, REPORT_MONTH)
