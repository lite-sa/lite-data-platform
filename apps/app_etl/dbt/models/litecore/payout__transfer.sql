-- Litecore mirror of raw payout__transfer:
-- the latest version of every row, PII columns removed. The column list is
-- an allowlist: a new source column reaches this table only when added here.
--
-- Left out: incoming_details: the ANB statement line behind an INCOMING
-- credit (source IBAN and account number, ordering party, beneficiary
-- name, narrative); comment: free text (trim map), though every prod value
-- so far is system-written ("Transfer to beneficiary", "Settlement
-- <timestamp>", a manual-failure note; probe 2026-09-30), so one flag
-- derived from it passes: is_beneficiary_transfer, the SAMA EMI payout
-- filter; initiated_by, checker_id: user identifiers (trim map).
{% set transfer = source('litecore', 'payout__transfer') -%}
{%- set transfer_columns = adapter.get_columns_in_relation(transfer) | map(attribute="name") | map("lower") | list -%}
select
    {{
        keep_columns(
            transfer,
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
    }},
    {% if "comment" in transfer_columns -%}
        comment like '%Transfer%'
    {%- else -%}
        cast(null as bool)
    {%- endif %} as is_beneficiary_transfer
from {{ transfer }}
qualify
    row_number() over (
        partition by id order by updated_at desc, _dlt_load_id desc
    )
    = 1
