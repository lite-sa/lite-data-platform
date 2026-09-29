"""Section I.1: ECOM merchants, total, active and inactive.

Population: merchants with KYB approved by month end (pi.common.KYB_MERCHANTS),
since approval opens the ECOM account. Active: at least one authorized ECOM
payment in the report month. Check column: ECOM payers outside the
population (should be 0).
"""

from app_etl.export.sama.common import run, show
from app_etl.export.sama.pi.common import AUTHORIZED_PAYMENTS, KYB_MERCHANTS

REPORT_MONTH = "2026-08"

DETAIL_SQL = f"""
with ecom as (
    select
        merchant_id,
        any_value(merchant_name) as merchant_name,
        any_value(is_test) as is_test,
        count(*) as payments_in_month,
        sum(amount_minor) as volume_in_month_minor,
        min(authorized_at) as first_payment_at,
        max(authorized_at) as last_payment_at
    from {AUTHORIZED_PAYMENTS}
    where channel = 'ECOM'
    group by merchant_id
)
select
    coalesce(m.merchant_id, e.merchant_id) as merchant_id,
    coalesce(m.merchant_name, e.merchant_name) as merchant_name,
    coalesce(m.is_test, e.is_test) as is_test,
    m.kyb_approved_on,
    m.kyb_date_is_created_at,
    m.merchant_id is not null as in_population,
    coalesce(e.payments_in_month, 0) as payments_in_month,
    coalesce(e.volume_in_month_minor, 0) / 100 as volume_in_month_sar,
    e.first_payment_at,
    e.last_payment_at,
    coalesce(e.payments_in_month, 0) > 0 as is_active
from {KYB_MERCHANTS} as m
full join ecom as e on e.merchant_id = m.merchant_id
order by payments_in_month desc, merchant_name
"""

ANSWER_SQL = f"""
select
    countif(in_population) as ecom_merchants,
    countif(in_population and is_active) as active,
    countif(in_population and not is_active) as inactive,
    countif(not in_population and is_active) as active_outside_population
from ({DETAIL_SQL})
"""

if __name__ == "__main__":
    answer = run(ANSWER_SQL, REPORT_MONTH)
    detail = run(DETAIL_SQL, REPORT_MONTH)
    show(answer, detail, REPORT_MONTH)
