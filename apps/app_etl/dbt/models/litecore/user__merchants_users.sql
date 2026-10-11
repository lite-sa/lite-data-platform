-- Litecore mirror of raw user__merchants_users:
-- the latest version of every row. The column list is an allowlist: a new
-- source column reaches this table only when added here.
--
-- Left out: nothing. email (the seat holder's address) and the created_by /
-- deleted_by user ids pass by ruling 2026-10-11.
select
    {{
        keep_columns(
            source('litecore', 'user__merchants_users'),
            [
                'id',
                'merchant_id',
                'user_id',
                'role_id',
                'status',
                'email',
                'email_verified',
                'created_by',
                'is_deleted',
                'deleted_by',
                'deleted_at',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'user__merchants_users') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
