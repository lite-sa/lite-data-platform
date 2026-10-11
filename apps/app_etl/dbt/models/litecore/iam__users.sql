-- Litecore mirror of raw iam__users:
-- the latest version of every row, PII columns removed. The column list is
-- an allowlist: a new source column reaches this table only when added here.
--
-- Left out: national_id, hijri_birth_date: masked instead (ruling
-- 2026-10-11), as national_id_masked and hijri_birth_date_masked;
-- yakeen_data: the full Yakeen payload (names, birth date, nationality, id
-- number, address), reduced to has_yakeen_data; email, phone_number,
-- full_name, national_address: the person; last_login_lat,
-- last_login_long: a location; metadata: JSON never reviewed ('{}' on
-- every prod row 2026-10-11); zitadel_user_id: identity-provider id.
{% set users = source('litecore', 'iam__users') -%}
with latest as (
    select
        {{
            keep_columns(
                users,
                [
                    'id',
                    'kyc_status',
                    'is_phone_verified',
                    'is_email_verified',
                    'last_login_country',
                    'created_at',
                    'updated_at',
                ]
            )
        }},
        {{ column_or_null(users, 'national_id', 'string') }},
        {{ column_or_null(users, 'hijri_birth_date', 'string') }},
        {{ column_or_null(users, 'yakeen_data', 'string') }}
    from {{ users }}
    qualify
        row_number() over (
            partition by id order by updated_at desc, _dlt_load_id desc
        )
        = 1
)
select
    * except (national_id, hijri_birth_date, yakeen_data),
    {{ masked('national_id', 2, 2) }} as national_id_masked,
    {{ masked('hijri_birth_date', 2, 1) }} as hijri_birth_date_masked,
    ifnull(national_id, '') != '' as has_national_id,
    ifnull(yakeen_data, '') not in ('', '{}', 'null') as has_yakeen_data
from latest
