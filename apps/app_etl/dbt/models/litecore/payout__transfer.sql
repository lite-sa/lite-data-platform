-- Litecore mirror of raw payout__transfer:
-- the latest version of every row, PII columns removed. The column list is
-- an allowlist: a new source column reaches this table only when added here.
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
