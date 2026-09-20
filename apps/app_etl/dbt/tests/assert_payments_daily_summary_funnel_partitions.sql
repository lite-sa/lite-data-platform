-- authorized / declined / no_decision must partition request_count.
select *
from {{ ref('payments_daily_summary') }}
where authorized_count + declined_count + no_decision_count != request_count
