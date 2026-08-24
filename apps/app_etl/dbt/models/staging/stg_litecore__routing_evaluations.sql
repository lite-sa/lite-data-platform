-- Latest version per smart-routing evaluation, standard stg_ dedup
-- contract. Grain: routing_evaluation_id — one COMPLETED row per authorize
-- in the happy path plus a retry tail; readers pick the first per payment.
--
-- Two prod facts baked in (nb 022 findings, re-verified in nb 023):
-- operation_type is stored lowercase ('authorize'), and the persisted
-- routing_obj keys are snake_case (primary.connector_type) — the DTO's
-- camelCase never reaches the database.
with
    latest as (

        select
            id as routing_evaluation_id,
            payment_id,
            operation_type,
            status,
            merchant_id,
            {{ column_or_null(source('litecore', 'smart_routing__transaction_evaluation'), 'gateway_id', 'string') }},
            {{ column_or_null(source('litecore', 'smart_routing__transaction_evaluation'), 'routing_obj', 'string') }},
            {{ column_or_null(source('litecore', 'smart_routing__transaction_evaluation'), 'matched_rules', 'string') }},
            created_at,
            updated_at

        from {{ source('litecore', 'smart_routing__transaction_evaluation') }}
        qualify
            row_number() over (
                partition by id order by updated_at desc, _dlt_load_id desc
            )
            = 1

    )

select
    * except (routing_obj, matched_rules),
    json_value(routing_obj, '$.primary.connector_type') as routed_connector,
    json_value(routing_obj, '$.primary.provider_config_id') as routed_provider_config,
    contains_substr(matched_rules, 'profile-fallback') as routed_via_fallback
from latest
