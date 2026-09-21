-- Litecore mirror of raw smart_routing__profile:
-- the latest version of every row, PII columns removed. The column list is
-- an allowlist: a new source column reaches this table only when added here.
--
-- Left out: description: free text.
select
    {{
        keep_columns(
            source('litecore', 'smart_routing__profile'),
            [
                'id',
                'merchant_id',
                'name',
                'status',
                'is_default',
                'is_deleted',
                'fallback_provider_config_id',
                'fallback_connector_type',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'smart_routing__profile') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
