-- Litecore mirror of raw iam__user_assignments:
-- the latest version of every row. The column list is an allowlist: a new
-- source column reaches this table only when added here.
--
-- Left out: nothing.
select
    {{
        keep_columns(
            source('litecore', 'iam__user_assignments'),
            [
                'id',
                'identity_id',
                'resource_type',
                'resource_id',
                'role',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'iam__user_assignments') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
