-- The 3DS FSM and the protocol verdict must agree on authentication
-- success: status AUTHENTICATED <=> transStatus Y/A (nb 019's guarded
-- overlap). A row here means the source contract drifted — revisit the
-- threeds derivation in the payments mart before trusting
-- threeds_authenticated.

select
    payment_id,
    threeds_status,
    threeds_trans_status,
    threeds_authenticated
from {{ ref('payments') }}
where
    has_threeds
    and (threeds_status = 'AUTHENTICATED') is distinct from threeds_authenticated
