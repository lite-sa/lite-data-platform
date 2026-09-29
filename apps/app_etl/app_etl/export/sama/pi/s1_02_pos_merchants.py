"""Section I.2: in-store POS merchants, total, active and inactive.

Population: merchants owning a terminal in service in the month
(pi.common.TERMINALS), plus any merchant that took a POS payment in the
month without one (the inventory shows only a terminal's current
merchant). Active: at least one authorized POS payment in the report month.
Check column: active merchants without a terminal in the inventory.
"""

from app_etl.export.sama.common import run, show
from app_etl.export.sama.pi.common import AUTHORIZED_PAYMENTS, TERMINALS

REPORT_MONTH = "2026-08"

DETAIL_SQL = f"""
with terminals as (
    select
        merchant_id,
        any_value(merchant_name) as merchant_name,
        any_value(is_test) as is_test,
        count(*) as terminals,
        string_agg(distinct terminal_type, ', ') as terminal_types
    from {TERMINALS}
    group by merchant_id
),
pos as (
    select
        merchant_id,
        any_value(merchant_name) as merchant_name,
        any_value(is_test) as is_test,
        count(*) as payments_in_month,
        sum(amount_minor) as volume_in_month_minor,
        min(authorized_at) as first_payment_at,
        max(authorized_at) as last_payment_at
    from {AUTHORIZED_PAYMENTS}
    where channel = 'POS'
    group by merchant_id
)
select
    coalesce(t.merchant_id, p.merchant_id) as merchant_id,
    coalesce(t.merchant_name, p.merchant_name) as merchant_name,
    coalesce(t.is_test, p.is_test) as is_test,
    t.merchant_id is not null as has_terminal,
    coalesce(t.terminals, 0) as terminals,
    t.terminal_types,
    coalesce(p.payments_in_month, 0) as payments_in_month,
    coalesce(p.volume_in_month_minor, 0) / 100 as volume_in_month_sar,
    p.first_payment_at,
    p.last_payment_at,
    coalesce(p.payments_in_month, 0) > 0 as is_active
from terminals as t
full join pos as p on p.merchant_id = t.merchant_id
where t.merchant_id is not null or p.payments_in_month > 0
order by payments_in_month desc, merchant_name
"""

ANSWER_SQL = f"""
select
    count(*) as pos_merchants,
    countif(is_active) as active,
    countif(not is_active) as inactive,
    countif(not has_terminal) as active_without_terminal
from ({DETAIL_SQL})
"""

if __name__ == "__main__":
    answer = run(ANSWER_SQL, REPORT_MONTH)
    detail = run(DETAIL_SQL, REPORT_MONTH)
    show(answer, detail, REPORT_MONTH)
