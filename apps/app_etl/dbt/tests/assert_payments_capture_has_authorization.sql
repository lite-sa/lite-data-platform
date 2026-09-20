-- A captured payment should also be authorized. A row here is an ecom
-- CAPTURE success whose AUTHORIZE success is missing. Warn severity.
{{ config(severity="warn") }}

select payment_id, channel, capture_mode, captured_at, authorized_at, outcome
from {{ ref('payments') }}
where captured_at is not null and authorized_at is null
