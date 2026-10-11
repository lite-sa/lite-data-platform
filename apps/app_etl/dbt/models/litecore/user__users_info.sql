-- Litecore mirror of raw user__users_info, flags only:
-- the latest version of every row, the identity record reduced to masks
-- and flags. The column list is an allowlist: a new source column reaches
-- this table only when added here.
--
-- Left out: national_id, higri_birth_date: masked instead (ruling
-- 2026-10-11), as national_id_masked and hijri_birth_date_masked;
-- national_address, focal_query_uuid: reduced to has_national_address and
-- has_focal_screening; id_expiration_date, yakeen_first_name,
-- yakeen_father_name, yakeen_grand_father_name, yakeen_family_name,
-- yakeen_full_name: the person's identity record.
{% set info = source('litecore', 'user__users_info') -%}
with latest as (
    select
        {{
            keep_columns(
                info,
                ['id', 'user_id', 'created_at', 'updated_at']
            )
        }},
        {{ column_or_null(info, 'national_id', 'string') }},
        {{ column_or_null(info, 'higri_birth_date', 'string') }},
        {{ column_or_null(info, 'national_address', 'string') }},
        {{ column_or_null(info, 'focal_query_uuid', 'string') }}
    from {{ info }}
    qualify
        row_number() over (
            partition by id order by updated_at desc, _dlt_load_id desc
        )
        = 1
)
select
    * except (national_id, higri_birth_date, national_address, focal_query_uuid),
    {{ masked('national_id', 2, 2) }} as national_id_masked,
    {{ masked('higri_birth_date', 2, 1) }} as hijri_birth_date_masked,
    ifnull(national_id, '') != '' as has_national_id,
    ifnull(national_address, '') not in ('', '[]', 'null') as has_national_address,
    focal_query_uuid is not null as has_focal_screening
from latest
