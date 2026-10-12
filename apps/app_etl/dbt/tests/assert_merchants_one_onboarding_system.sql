-- A merchant_id present in both business_management and lem. The
-- merchants registry keeps the lem row; this warns so the build goes on.
{{ config(severity="warn") }}

select merchants.merchant_id
from {{ ref('merchants') }} as merchants
join
    {{ source('litecore', 'business_management__business_entities') }} as bms
    on merchants.merchant_id = bms.business_id
where merchants.source_system = 'lem'
