-- Litecore mirror of raw lem__legal_entities:
-- the latest version of every row, PII columns removed. The column list is
-- an allowlist: a new source column reaches this table only when added here.
--
-- Left out: contact_phone_number, contact_mobile_number, contact_email:
-- they reach a person (trim map).
select
    {{
        keep_columns(
            source('litecore', 'lem__legal_entities'),
            [
                'id',
                'organization_id',
                'legal_name',
                'registration_number',
                'country_code',
                'website_url',
                'kyb_approved_at',
                'status',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'lem__legal_entities') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
