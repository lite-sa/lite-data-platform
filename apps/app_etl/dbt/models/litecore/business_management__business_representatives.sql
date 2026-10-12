-- Litecore mirror of raw business_management__business_representatives:
-- the latest version of every row, PII columns removed or masked. The
-- column list is an allowlist: a new source column reaches this table only
-- when added here.
--
-- Left out: national_id, date_of_birth: masked instead (ruling
-- 2026-10-11), as national_id_masked and date_of_birth_masked;
-- yakeen_full_name: reduced to has_yakeen_name; phone_number, name,
-- yakeen_first_name, yakeen_father_name, yakeen_grand_father_name,
-- yakeen_family_name: the person (trim map).
{% set reps = source('litecore', 'business_management__business_representatives') -%}
with latest as (
    select
        {{
            keep_columns(
                reps,
                [
                    'id',
                    'business_id',
                    'user_id',
                    'phone_number_verified',
                    'created_at',
                    'updated_at',
                ]
            )
        }},
        {{ column_or_null(reps, 'national_id', 'string') }},
        {{ column_or_null(reps, 'date_of_birth', 'string') }},
        {{ column_or_null(reps, 'yakeen_full_name', 'string') }}
    from {{ reps }}
    qualify
        row_number() over (
            partition by id order by updated_at desc, _dlt_load_id desc
        )
        = 1
)
select
    * except (national_id, date_of_birth, yakeen_full_name),
    {{ masked('national_id', 2, 2) }} as national_id_masked,
    {{ masked('date_of_birth', 2, 1) }} as date_of_birth_masked,
    ifnull(national_id, '') != '' as has_national_id,
    ifnull(yakeen_full_name, '') != '' as has_yakeen_name
from latest
