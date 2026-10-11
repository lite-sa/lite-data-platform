-- Litecore mirror of raw user__user_invitations:
-- the latest version of every row, secrets removed. The column list is an
-- allowlist: a new source column reaches this table only when added here.
--
-- Left out: token: a plaintext bearer token, live until used or expired.
-- state is derived: status stays 'pending' after expires_at passes.
{% set invitations = source('litecore', 'user__user_invitations') -%}
select
    {{
        keep_columns(
            invitations,
            [
                'id',
                'merchant_id',
                'user_id',
                'status',
                'used',
                'expires_at',
                'created_at',
                'updated_at',
            ]
        )
    }},
    case
        when status = 'pending' and expires_at < current_timestamp()
        then 'expired'
        else status
    end as state
from {{ invitations }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
