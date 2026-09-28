"""Section III: transaction count and volume by channel and card scheme.

Monthly. Payments authorized in the report month (pi.common.AUTHORIZED_PAYMENTS),
split POS / ECOM and MADA / VISA / MASTERCARD / Other. Other holds every
other scheme and payments with no scheme found (the unknown_scheme column).
The All rows cover every channel, so All must equal POS plus ECOM.
Refunds are not netted.
"""

from app_etl.export.sama.common import run, show
from app_etl.export.sama.pi.common import AUTHORIZED_PAYMENTS

REPORT_MONTH = "2026-08"

DETAIL_SQL = f"""
select *
from {AUTHORIZED_PAYMENTS}
order by authorized_at
"""

ANSWER_SQL = f"""
with
txns as (
    select channel, scheme_reported, scheme_source, amount_minor
    from ({DETAIL_SQL})
    where not is_test
),
grid as (
    select channel, channel_order, scheme, scheme_order
    from unnest(['POS', 'ECOM', 'All']) as channel with offset as channel_order
    cross join
        unnest(['MADA', 'VISA', 'MASTERCARD', 'Other', 'Total']) as scheme
        with offset as scheme_order
)
select
    g.channel,
    g.scheme,
    count(t.amount_minor) as transactions,
    round(coalesce(sum(t.amount_minor), 0) / 100, 2) as volume_sar,
    countif(t.scheme_source = 'unknown') as unknown_scheme
from grid as g
left join txns as t
    on g.channel in (t.channel, 'All') and g.scheme in (t.scheme_reported, 'Total')
group by g.channel, g.scheme, g.channel_order, g.scheme_order
order by g.channel_order, g.scheme_order
"""

if __name__ == "__main__":
    answer = run(ANSWER_SQL, REPORT_MONTH)
    detail = run(DETAIL_SQL, REPORT_MONTH)
    show(answer, detail, REPORT_MONTH)
