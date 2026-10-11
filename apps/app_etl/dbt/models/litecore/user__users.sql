-- Litecore mirror of raw user__users:
-- the latest version of every row, PII columns removed. The column list is
-- an allowlist: a new source column reaches this table only when added here.
--
-- Left out: phone_number, full_name: the person; last_login_lat,
-- last_login_long: a location precise to a building (last_login_country
-- stays). user_id, the identity-provider id, passes (ruling 2026-10-11).
select
    {{
        keep_columns(
            source('litecore', 'user__users'),
            [
                'id',
                'user_id',
                'user_type',
                'status',
                'phone_number_verified',
                'failed_login_attempts',
                'is_locked',
                'locked_at',
                'last_login_at',
                'last_login_country',
                'kyc_status',
                'kyc_completed_at',
                'is_deleted',
                'deleted_at',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'user__users') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
