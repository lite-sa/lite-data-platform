-- Litecore mirror of raw lem__agreements:
-- the latest version of every row, PII columns removed. The column list is
-- an allowlist: a new source column reaches this table only when added here.
--
-- Left out: phone_number, member_name: the contact invited when the target
-- account is not onboarded yet, a person.
select
    {{
        keep_columns(
            source('litecore', 'lem__agreements'),
            [
                'id',
                'source_business_account_id',
                'target_business_account_id',
                'agreement_type',
                'agreement_rules',
                'status',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'lem__agreements') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
