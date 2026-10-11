-- Litecore mirror of raw user__merchants:
-- the latest version of every row. The column list is an allowlist: a new
-- source column reaches this table only when added here.
--
-- Left out: name: blank on 63% of prod rows (2026-10-11); the
-- business_entities name is the one to use.
select
    {{
        keep_columns(
            source('litecore', 'user__merchants'),
            [
                'id',
                'merchant_id',
                'status',
                'is_deleted',
                'deleted_at',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'user__merchants') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
