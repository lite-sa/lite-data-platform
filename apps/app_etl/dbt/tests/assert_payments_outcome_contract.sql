-- The outcome contract, nb 023 §9's hard asserts as a test: authorized ⇔
-- a decision-family success op exists (authorized_at), and every declined
-- payment carries a failure_category — the taxonomy may not silently
-- drop declines. A row here means the decision derivation drifted — fix
-- the fact before trusting any auth rate built on it. (The soft
-- capture-implies-authorization check lives in its own warn-severity
-- test.)
select
    payment_id,
    outcome,
    authorized_at,
    failure_category
from {{ ref('payments') }}
where
    (outcome = 'authorized') != (authorized_at is not null)
    or (outcome = 'declined' and failure_category is null)
