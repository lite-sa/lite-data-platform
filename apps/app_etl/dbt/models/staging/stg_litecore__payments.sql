-- Rename/select only, deduped to the latest version per key. Raw is
-- append-only — ingestion re-extracts a row on every source UPDATE, so raw
-- holds one row per (id, updated_at) version and every reader must come
-- through this dedup or joins fan out.

select
    id as payment_id,
    merchant_id,
    amount as amount_minor,
    currency,
    status,
    payment_method,
    channel_id,
    created_at,
    datetime(created_at, '{{ var("aml_local_timezone") }}') as created_at_local,
    updated_at

from {{ source('litecore', 'payments') }}
qualify
    row_number() over (partition by id order by updated_at desc, _dlt_load_id desc) = 1
