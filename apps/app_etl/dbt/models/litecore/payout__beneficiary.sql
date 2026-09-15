-- Litecore mirror of raw payout__beneficiary:
-- the latest version of every row (raw appends one row per source-row
-- version), PII columns removed, one table for the payments team in
-- Metabase. The column list is the allowlist
-- (docs/schema-management.md §1): a new source column reaches this table
-- only by being added here after a look at the trim map.
--
-- Left out: details: the destination IBAN, BIC and address; name: the
-- merchant-typed beneficiary name, often a person; creditor_party_name:
-- the bank-verified account-holder name; created_by, checker_id: user
-- identifiers (trim map).
select
    {{
        keep_columns(
            source('litecore', 'payout__beneficiary'),
            [
                'id',
                'merchant_id',
                'instrument_id',
                'type',
                'tag',
                'status',
                'external_status',
                'external_reference',
                'is_deleted',
                'maker_country_code',
                'otp_sent_at',
                'otp_expires_at',
                'otp_verified',
                'checker_decided_at',
                'created_at',
                'updated_at',
            ]
        )
    }}
from {{ source('litecore', 'payout__beneficiary') }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
