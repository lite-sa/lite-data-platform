-- Rename/select only, deduped to the latest version per key. Raw is
-- append-only — ingestion re-extracts a row on every source UPDATE, so raw
-- holds one row per (id, updated_at) version and every reader must come
-- through this dedup or joins fan out.
--
-- instrument_data / channel_type landed at source 2026-08-17/18 — older
-- versions carry NULL (the mart's channel fallback covers them). JSONB keys
-- are snake_case at rest (CaseMapper runs before insert; confirmed on prod
-- rows, nb 023 — the TypeScript camelCase names return NULL). Only scalar
-- card facts are extracted; the instrument_data blob itself never leaves
-- staging, and the same rule holds for risk_result (presence + two keys)
-- and threeds/routing results (presence only). card_last_four is
-- masked-PAN display data (receipt-grade, not the PAN) — the card-level
-- join key nb 023 uses alongside card_brand.
with
    latest as (

        select
            id as payment_id,
            merchant_id,
            amount as amount_minor,
            currency,
            status,
            payment_method,
            {{ column_or_null(source('litecore', 'payment_v2__payments'), 'capture_mode', 'string') }},
            {{ column_or_null(source('litecore', 'payment_v2__payments'), 'processing_type', 'string') }},
            {{ column_or_null(source('litecore', 'payment_v2__payments'), 'channel_type', 'string') }},
            {{ column_or_null(source('litecore', 'payment_v2__payments'), 'payment_link_id', 'string') }},
            {{ column_or_null(source('litecore', 'payment_v2__payments'), 'order_id', 'string') }},
            {{ column_or_null(source('litecore', 'payment_v2__payments'), 'instrument_data', 'string') }},
            {{ column_or_null(source('litecore', 'payment_v2__payments'), 'gateway_reference_id', 'string') }},
            risk_result is not null as has_risk_result,
            json_value(risk_result, '$.allowed') = 'true' as risk_allowed,
            json_value(risk_result, '$.requires_threeds') = 'true' as risk_requires_threeds,
            routing_result is not null as has_routing_result,
            threeds_result is not null as has_threeds_result,
            channel_id,
            created_at,
            datetime(created_at, '{{ var("local_timezone") }}') as created_at_local,
            updated_at

        from {{ source('litecore', 'payment_v2__payments') }}
        qualify
            row_number() over (
                partition by id order by updated_at desc, _dlt_load_id desc
            )
            = 1

    )

select
    * except (instrument_data),
    json_value(instrument_data, '$.card_brand') as card_brand,
    json_value(instrument_data, '$.type') as instrument_type,
    json_value(instrument_data, '$.last_four') as card_last_four,
    json_value(instrument_data, '$.future_usage') as future_usage,
    json_value(instrument_data, '$.expiry_year') as card_expiry_year,
    json_value(instrument_data, '$.expiry_month') as card_expiry_month
from latest
