-- Litecore mirror of raw payout__transfer:
-- the latest version of every row (raw appends one row per source-row
-- version), PII columns removed, one table for the payments team in
-- Metabase. The column list is the allowlist
-- (docs/schema-management.md §1): a new source column reaches this table
-- only by being added here after a look at the trim map.
--
-- Left out: incoming_details: the ANB statement line behind an INCOMING
-- credit (source IBAN and account number, ordering party, beneficiary
-- name, narrative); comment: merchant free text; initiated_by,
-- checker_id: user identifiers (trim map).
select
    {{
        keep_columns(
            source('litecore', 'payout__transfer'),
            [
                'id',
                'direction',
                'merchant_id',
                'beneficiary_id',
                'instruction_settlement_id',
                'external_unique_id',
                'amount',
                'total_amount',
                'fees',
                'currency',
                'status',
                'value_date',
                'otp_sent_at',
                'otp_expires_at',
                'otp_verified',
                'checker_decision_at',
                'processed_at',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'payout__transfer') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
