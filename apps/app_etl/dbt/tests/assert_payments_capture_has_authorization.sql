-- A capture witness should imply an authorization witness: POS captures
-- are decision ops themselves and the ecom INSTANT fold sets captured_at
-- from authorized_at, so the only way here is an ecom CAPTURE/SUCCESS on
-- a payment whose AUTHORIZE success is missing. nb 023 kept this soft
-- (zero rows on prod at first build) — warn, don't block: a row means a
-- source edge worth reading, not necessarily a broken build.
{{ config(severity="warn") }}

select payment_id, channel, capture_mode, captured_at, authorized_at, outcome
from {{ ref('payments') }}
where captured_at is not null and authorized_at is null
