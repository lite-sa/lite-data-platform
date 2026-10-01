-- Litecore mirror of raw checkout_session__checkout_sessions:
-- the latest version of every row, PII columns removed. The column list is
-- an allowlist: a new source column reaches this table only when added here.
--
-- Left out: customer: contact JSONB, only a hash of its id survives as
-- customer_id_hash; metadata, order_data: merchant JSONB;
-- payment_url: a live capability URL until expires_on; redirect_urls: 90% carry
-- a query-string token; idempotency_key: free text, 2 prod values look like an
-- email or a phone (trim map). splits: unreviewed, NULL on every prod row.
{%- set src = source('litecore', 'checkout_session__checkout_sessions') -%}
{#- dlt creates a raw column only once a non-null value lands, so dev can
    lack customer; the hash is then a typed NULL instead of a broken build. -#}
{%- set has_customer = execute and 'customer' in (adapter.get_columns_in_relation(src) | map(attribute='name') | map('lower') | list) -%}
select
    {{
        keep_columns(
            source('litecore', 'checkout_session__checkout_sessions'),
            [
                'id',
                'merchant_id',
                'acting_business_account_id',
                'order_id',
                'status',
                'amount',
                'currency',
                'three_ds_required',
                'processing_type',
                'channel_id',
                'expires_on',
                'created_at',
                'updated_at',
            ]
        )
    }},
    -- customer.id is the merchant's own reference, free text that can be a
    -- phone or an email, so only its hash lands. Scoped by merchant so the
    -- same raw id at two merchants stays apart. NULL when no id was sent.
    {% if has_customer -%}
    to_hex(sha256(concat(merchant_id, ':', json_value(customer, '$.id'))))
    {%- else -%}
    cast(null as string)
    {%- endif %} as customer_id_hash
from {{ src }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
