-- Litecore mirror of raw payout__beneficiary_transaction:
-- the latest version of every row, PII columns removed. The column list is
-- an allowlist: a new source column reaches this table only when added here.
--
-- Left out: request, response: the ANB account-verification payloads
-- (IBAN, receiver bank, the verified account-holder name, request
-- headers); comment: free text (trim map).
select
    {{
        keep_columns(
            source('litecore', 'payout__beneficiary_transaction'),
            [
                'id',
                'beneficiary_id',
                'status',
                'external_status',
                'external_reference',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'payout__beneficiary_transaction') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
