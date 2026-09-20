-- Litecore mirror of raw settlement__account:
-- the latest version of every row, PII columns removed. The column list is
-- an allowlist: a new source column reaches this table only when added here.
--
-- Left out: created_by, updated_by: staff identifiers (trim map).
select
    {{
        keep_columns(
            source('litecore', 'settlement__account'),
            [
                'id',
                'merchant_id',
                'type',
                'entity_id',
                'status',
                'currency',
                'is_deleted',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'settlement__account') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
