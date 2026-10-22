-- One row per payment_id × mode.
select payment_id, mode, count(*) as row_count
from {{ ref('risk_decisions') }}
group by payment_id, mode
having count(*) > 1
