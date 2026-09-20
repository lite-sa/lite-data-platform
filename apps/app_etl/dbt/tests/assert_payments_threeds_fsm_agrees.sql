-- The 3DS FSM and the protocol verdict must agree: status AUTHENTICATED
-- <=> transStatus Y/A.

select
    payment_id,
    threeds_status,
    threeds_trans_status,
    threeds_authenticated
from {{ ref('payments') }}
where
    has_threeds
    and (threeds_status = 'AUTHENTICATED') is distinct from threeds_authenticated
