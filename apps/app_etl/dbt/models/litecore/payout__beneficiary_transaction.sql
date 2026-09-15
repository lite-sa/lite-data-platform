-- Litecore mirror of raw payout__beneficiary_transaction:
-- the latest version of every row (raw appends one row per source-row
-- version), PII columns removed, one table for the payments team in
-- Metabase. The column list is the allowlist
-- (docs/schema-management.md §1): a new source column reaches this table
-- only by being added here after a look at the trim map.
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
