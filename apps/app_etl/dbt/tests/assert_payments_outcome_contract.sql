-- The outcome contract: authorized <=> authorized_at is set, and every
-- declined payment carries a failure_category.
select
    payment_id,
    outcome,
    authorized_at,
    failure_category
from {{ ref('payments') }}
where
    (outcome = 'authorized') != (authorized_at is not null)
    or (outcome = 'declined' and failure_category is null)
