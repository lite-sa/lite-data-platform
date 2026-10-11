-- Litecore mirror of raw iam__identities:
-- the latest version of every row. The column list is an allowlist: a new
-- source column reaches this table only when added here.
--
-- Left out: nothing.
select
    {{
        keep_columns(
            source('litecore', 'iam__identities'),
            [
                'id',
                'type',
                'status',
                'created_at',
                'updated_at',
                'deleted_at',
            ]
        )
    }}
from {{ source('litecore', 'iam__identities') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
