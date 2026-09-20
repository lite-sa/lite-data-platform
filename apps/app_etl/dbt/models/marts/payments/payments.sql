{{
    config(
        materialized="table",
        partition_by={"field": "created_at", "data_type": "timestamp", "granularity": "day"},
        cluster_by=["merchant_id"],
    )
}}

-- One row per payment_id: entry-mode classification, card facts, the
-- decision from the op log, 3DS and routing milestones, the outcome and
-- failure taxonomy (decline_code_map seed), and the milestone funnel string.
--
-- Full rebuild from the staging views on every dbt build, so late mutations
-- restate and reruns are idempotent. The partition / cluster layout already
-- fits an incremental merge on payment_id if the daily scan grows.
--
-- Test merchants and incident-remediated payments are excluded through the
-- test_merchants and incident_excluded_payments seeds. Reads only staging
-- views: a new column goes into a staging view first.
--
-- Decision contract: the decision op family is AUTHORIZE on ecom and
-- AUTHORIZE + CAPTURE on POS (a POS sale persists as a CAPTURE op with no
-- AUTHORIZE). The first SUCCESS in the family is the decision and sets
-- authorized_at. The last FAILURE in the family carries the failure facts.
with

    test_merchants as (select merchant_id from {{ ref('test_merchants') }}),

    incident_exclusions as (
        select payment_id from {{ ref('incident_excluded_payments') }}
    ),

    spine as (

        select
            pay.payment_id,
            pay.merchant_id,
            pay.amount_minor,
            pay.currency,
            pay.status as source_status,
            pay.payment_method,
            pay.capture_mode,
            pay.processing_type,
            pay.channel_type,
            pay.payment_link_id,
            pay.order_id,
            pay.card_brand,
            pay.instrument_type,
            pay.card_last_four,
            pay.future_usage,
            pay.card_expiry_year,
            pay.card_expiry_month,
            pay.gateway_reference_id,
            pay.has_risk_result,
            pay.risk_allowed,
            pay.risk_requires_threeds,
            pay.has_routing_result,
            pay.has_threeds_result,
            pay.channel_id,
            pay.created_at,
            pay.created_at_local,
            pay.updated_at as source_updated_at
        from {{ ref('stg_litecore__payments') }} as pay
        left join test_merchants as tm on pay.merchant_id = tm.merchant_id
        left join incident_exclusions as ix on pay.payment_id = ix.payment_id
        where tm.merchant_id is null and ix.payment_id is null

    ),

    ops as (select * from {{ ref('stg_litecore__payment_operations') }}),

    -- Op witnesses for the channel fallback: only POS ops carry terminal_id.
    op_flags as (

        select
            payment_id,
            logical_or(terminal_id is not null) as any_pos_op,
            logical_or(terminal_id is null) as any_ecom_op,
            count(*) as n_ops,
            countif(status = 'SUCCESS') as n_success_ops,
            countif(status = 'FAILURE') as n_failure_ops,
            countif(status = 'PENDING') as n_pending_ops,
            -- PURCHASE is a known raw literal (normalized to CAPTURE
            -- downstream); anything else outside the enum is a tripwire
            countif(
                operation_type not in (
                    'AUTHORIZE',
                    'CAPTURE',
                    'PURCHASE',
                    'REFUND',
                    'VOID',
                    'EXTEND_AUTHORIZATION',
                    'REVERSE'
                )
            ) as n_unknown_op_types
        from ops
        group by payment_id

    ),

    -- 3DS facts and milestone clocks of the last authentication attempt per
    -- payment. trans_status (Y/A = authenticated) is the source of truth;
    -- the FSM status adds EXPIRED (shopper abandonment). A singular test
    -- guards AUTHENTICATED <=> Y/A.
    threeds as (

        select
            payment_id,
            count(*) over (partition by payment_id) as n_threeds_attempts,
            status as threeds_status,
            trans_status as threeds_trans_status,
            eci as threeds_eci,
            authentication_scheme,
            trans_status in ('Y', 'A') as threeds_authenticated,
            acs_challenge_mandated = 'Y' as threeds_challenge_mandated,
            status = 'EXPIRED' as threeds_abandoned,
            created_at as threeds_started_at,
            if(challenge_presented, created_at, null) as challenge_presented_at,
            if(
                challenge_presented and status in ('AUTHENTICATED', 'FAILED'),
                updated_at,
                null
            ) as challenge_completed_at,
            if(status = 'EXPIRED', updated_at, null) as challenge_abandoned_at,
            if(trans_status in ('Y', 'A'), updated_at, null) as authenticated_at,
            if(status = 'FAILED', updated_at, null) as threeds_failed_at,
            -- the auth FSM's finalize window; masked for EXPIRED rows, where
            -- it measures the expiry sweep, not shopper behaviour
            if(
                status = 'EXPIRED',
                null,
                timestamp_diff(updated_at, created_at, millisecond) / 1000
            ) as threeds_duration_s
        from {{ ref('stg_litecore__threeds') }}
        where payment_id is not null
        qualify
            row_number() over (
                partition by payment_id order by created_at desc, threeds_id desc
            )
            = 1

    ),

    -- Channel first, then the entry mode within it. channel_type decides
    -- where present (since 2026-08-18); older rows fall back to the op
    -- witnesses, and a threeds row marks an op-less payment as ecom. The
    -- entry-mode CASE is first-match: the facts overlap, so order matters.
    classified as (

        select
            spine.*,
            merchants.merchant_name,
            case
                when spine.channel_type = 'IN-PERSON' or coalesce(op_flags.any_pos_op, false)
                then 'pos'
                when
                    spine.channel_type = 'ECOM'
                    or coalesce(op_flags.any_ecom_op, false)
                    or threeds.payment_id is not null
                then 'ecom'
                else 'unknown'
            end as channel,
            coalesce(op_flags.n_ops, 0) as n_ops,
            coalesce(op_flags.n_success_ops, 0) as n_success_ops,
            coalesce(op_flags.n_failure_ops, 0) as n_failure_ops,
            coalesce(op_flags.n_pending_ops, 0) as n_pending_ops,
            coalesce(op_flags.n_unknown_op_types, 0) as n_unknown_op_types
        from spine
        left join
            {{ ref('stg_litecore__business_entities') }} as merchants
            on spine.merchant_id = merchants.merchant_id
        left join op_flags on spine.payment_id = op_flags.payment_id
        left join threeds on spine.payment_id = threeds.payment_id

    ),

    entry_modes as (

        select
            payment_id,
            case
                when channel = 'pos'
                then 'pos'
                when channel = 'unknown'
                then 'unknown'
                -- MOTO before payment_link: a link-delivered MOTO payment
                -- is a MOTO payment.
                when processing_type = 'MOTO'
                then 'moto'
                when payment_link_id is not null
                then 'payment_link'
                when processing_type in ('CARD_ON_FILE', 'UNSCHEDULED_CARD_ON_FILE')
                then 'stored_credential_mit'
                when payment_method in ('APPLE_PAY', 'GOOGLE_PAY', 'SAMSUNG_PAY')
                then 'ecom_wallet'
                when future_usage is not null
                then 'stored_credential_cit'
                else 'ecom_card'
            end as entry_mode
        from classified

    ),

    -- Ops with their payment's channel and the PURCHASE→CAPTURE
    -- normalization; decision_ops narrows to the per-pipeline family.
    ops_classified as (

        select
            ops.*,
            if(ops.operation_type = 'PURCHASE', 'CAPTURE', ops.operation_type) as op_family,
            classified.channel,
            classified.capture_mode
        from ops
        inner join classified on ops.payment_id = classified.payment_id

    ),

    decision_ops as (

        select *
        from ops_classified
        where
            if(
                channel = 'pos',
                op_family in ('AUTHORIZE', 'CAPTURE'),
                op_family = 'AUTHORIZE'
            )

    ),

    -- First decision SUCCESS, ordered by the op's terminal moment
    -- (updated_at).
    first_success as (

        select
            payment_id,
            updated_at as authorized_at,
            connector_type as auth_connector,
            connector_transaction_id as auth_transaction_id,
            processing_duration_ms as auth_duration_ms,
            initiated_by as auth_initiated_by,
            external_operation_type as auth_external_op,
            verification_method as auth_verification
        from decision_ops
        where status = 'SUCCESS'
        qualify
            row_number() over (
                partition by payment_id
                order by updated_at asc, created_at asc, payment_operation_id asc
            )
            = 1

    ),

    -- Last decision FAILURE — the failure facts (final decline for declined
    -- payments; retry noise for authorized ones, kept out of the funnel).
    last_failure as (

        select
            payment_id,
            updated_at as failed_at,
            reason as failure_reason,
            connector_type as failure_connector,
            connector_response_code as failure_response_code,
            connector_response_message as failure_response_message,
            http_status_code as failure_http_status,
            processing_duration_ms as failure_duration_ms,
            receipt_outcome as failure_outcome,
            frontend_error as failure_frontend_error,
            initiated_by as failure_initiated_by
        from decision_ops
        where status = 'FAILURE'
        qualify
            row_number() over (
                partition by payment_id
                order by updated_at desc, created_at desc, payment_operation_id desc
            )
            = 1

    ),

    decision_counts as (

        select payment_id, countif(status = 'FAILURE') as n_decision_failures
        from decision_ops
        group by payment_id

    ),

    captures as (

        select
            payment_id,
            min(updated_at) as first_capture_at,
            sum(amount_minor) as captured_total_raw,
            count(*) as n_capture_success
        from ops_classified
        where op_family = 'CAPTURE' and status = 'SUCCESS'
        group by payment_id

    ),

    refunds as (

        select
            payment_id,
            min(updated_at) as refunded_at,
            max(updated_at) as last_refunded_at,
            sum(amount_minor) as refunded_total,
            count(*) as n_refunds
        from ops_classified
        where op_family = 'REFUND' and status = 'SUCCESS'
        group by payment_id

    ),

    reversals as (

        select
            payment_id,
            min(updated_at) as reversed_at,
            sum(amount_minor) as reversed_total,
            count(*) as n_reversals
        from ops_classified
        where op_family = 'REVERSE' and status = 'SUCCESS'
        group by payment_id

    ),

    voids as (

        select payment_id, min(updated_at) as voided_at
        from ops_classified
        where op_family = 'VOID' and status = 'SUCCESS'
        group by payment_id

    ),

    extends as (

        select payment_id, min(updated_at) as extended_at
        from ops_classified
        where op_family = 'EXTEND_AUTHORIZATION' and status = 'SUCCESS'
        group by payment_id

    ),

    -- First authorize evaluation per payment; the happy path is one
    -- COMPLETED row, retries append.
    routing as (

        select
            payment_id,
            min(created_at) as routed_at,
            count(*) as n_routing_rows,
            countif(status != 'COMPLETED') as n_routing_not_completed,
            any_value(routed_connector having min created_at) as routed_connector,
            any_value(
                routed_provider_config having min created_at
            ) as routed_provider_config,
            logical_or(coalesce(routed_via_fallback, false)) as routed_via_fallback
        from {{ ref('stg_litecore__routing_evaluations') }}
        where lower(operation_type) = 'authorize'
        group by payment_id

    ),

    enriched as (

        select
            classified.*,
            entry_modes.entry_mode,
            first_success.authorized_at,
            first_success.auth_connector,
            first_success.auth_transaction_id,
            first_success.auth_duration_ms,
            first_success.auth_initiated_by,
            first_success.auth_external_op,
            first_success.auth_verification,
            last_failure.failed_at,
            last_failure.failure_reason,
            last_failure.failure_connector,
            last_failure.failure_response_code,
            last_failure.failure_response_message,
            last_failure.failure_http_status,
            last_failure.failure_duration_ms,
            last_failure.failure_outcome,
            coalesce(last_failure.failure_frontend_error, false) as failure_frontend_error,
            last_failure.failure_initiated_by,
            coalesce(decision_counts.n_decision_failures, 0) as n_decision_failures,
            captures.first_capture_at,
            captures.captured_total_raw,
            coalesce(captures.n_capture_success, 0) as n_capture_success,
            refunds.refunded_at,
            refunds.last_refunded_at,
            coalesce(refunds.refunded_total, 0) as refunded_total,
            coalesce(refunds.n_refunds, 0) as n_refunds,
            reversals.reversed_at,
            coalesce(reversals.reversed_total, 0) as reversed_total,
            coalesce(reversals.n_reversals, 0) as n_reversals,
            voids.voided_at,
            extends.extended_at,
            routing.routed_at,
            coalesce(routing.n_routing_rows, 0) as n_routing_rows,
            coalesce(routing.n_routing_not_completed, 0) as n_routing_not_completed,
            routing.routed_connector,
            routing.routed_provider_config,
            coalesce(routing.routed_via_fallback, false) as routed_via_fallback,
            coalesce(threeds.n_threeds_attempts, 0) as n_threeds_attempts,
            coalesce(threeds.n_threeds_attempts, 0) > 0 as has_threeds,
            threeds.threeds_status,
            threeds.threeds_trans_status,
            threeds.threeds_eci,
            threeds.authentication_scheme,
            coalesce(threeds.threeds_authenticated, false) as threeds_authenticated,
            coalesce(
                threeds.threeds_challenge_mandated, false
            ) as threeds_challenge_mandated,
            coalesce(threeds.threeds_abandoned, false) as threeds_abandoned,
            threeds.threeds_started_at,
            threeds.challenge_presented_at,
            threeds.challenge_completed_at,
            threeds.challenge_abandoned_at,
            threeds.authenticated_at,
            threeds.threeds_failed_at,
            threeds.threeds_duration_s
        from classified
        inner join entry_modes on classified.payment_id = entry_modes.payment_id
        left join first_success on classified.payment_id = first_success.payment_id
        left join last_failure on classified.payment_id = last_failure.payment_id
        left join decision_counts on classified.payment_id = decision_counts.payment_id
        left join captures on classified.payment_id = captures.payment_id
        left join refunds on classified.payment_id = refunds.payment_id
        left join reversals on classified.payment_id = reversals.payment_id
        left join voids on classified.payment_id = voids.payment_id
        left join extends on classified.payment_id = extends.payment_id
        left join routing on classified.payment_id = routing.payment_id
        left join threeds on classified.payment_id = threeds.payment_id

    ),

    -- Derived pass: canonical status, outcome, the failure category (POS:
    -- TMS reason / device error / receipt outcome / response code; ecom:
    -- reason strings, then the connector-null fallback), the single-message
    -- capture fold, and the gateway-called clock (ecom only).
    facts as (

        select
            enriched.*,
            if(
                source_status = 'PENDING'
                and n_failure_ops > 0
                and n_success_ops = 0,
                'FAILED',
                source_status
            ) as status,
            case
                when authorized_at is not null
                then 'authorized'
                when failed_at is not null
                then 'declined'
                else 'no_decision'
            end as outcome,
            case
                when failed_at is null
                then null
                when channel = 'pos'
                then
                    case
                        when failure_reason is not null
                        then 'pos_validation'
                        when failure_frontend_error
                        then 'pos_device_error'
                        when failure_outcome = 'CANCELLED'
                        then 'pos_cancelled'
                        when failure_response_code is not null
                        then 'pos_provider_declined'
                        else 'pos_no_verdict'
                    end
                when failure_reason = 'Risk evaluation failed'
                then 'risk_error'
                when failure_reason = 'Risk assessment rejected'
                then 'risk_rejected'
                when failure_reason = 'Routing decision failed'
                then 'routing_error'
                when failure_reason = '3DS authentication failed with status: FAILED'
                then 'threeds_failed'
                when failure_reason = '3DS authentication failed'
                then 'threeds_expired_or_error'
                when failure_reason = 'Authorization declined'
                then 'gateway_declined'
                when failure_connector is null
                then 'validation'
                else 'gateway_no_verdict'
            end as failure_category,
            if(
                channel = 'ecom' and capture_mode = 'INSTANT',
                authorized_at,
                first_capture_at
            ) as captured_at,
            case
                when channel = 'ecom' and capture_mode = 'INSTANT'
                then if(authorized_at is not null, amount_minor, 0)
                else coalesce(captured_total_raw, 0)
            end as captured_total,
            coalesce(auth_duration_ms, failure_duration_ms) as decision_duration_ms,
            if(
                channel = 'ecom',
                timestamp_sub(
                    coalesce(authorized_at, failed_at),
                    interval coalesce(auth_duration_ms, failure_duration_ms) millisecond
                ),
                null
            ) as gateway_called_at,
            coalesce(auth_connector, failure_connector) as gateway
        from enriched

    )

    -- Minor→major divisor per ISO 4217 exponent (NUMERIC division, exact).
    -- The exponent-3 Gulf currencies are listed so a BHD/KWD payment is not
    -- 10x off. A new currency trips the accepted_values test on currency.
    {% set minor_per_major = "if(currency in ('BHD', 'IQD', 'JOD', 'KWD', 'LYD', 'OMR', 'TND'), 1000, 100)" %}

select
    facts.payment_id,
    facts.merchant_id,
    facts.merchant_name,
    facts.status,
    facts.source_status,
    facts.outcome,
    facts.channel,
    facts.entry_mode,
    facts.channel_type,
    facts.processing_type,
    facts.payment_method,
    facts.capture_mode,
    facts.payment_link_id is not null as via_payment_link,
    facts.order_id,
    facts.card_brand,
    facts.instrument_type,
    facts.card_last_four,
    facts.future_usage,
    facts.card_expiry_year,
    facts.card_expiry_month,
    facts.amount_minor,
    cast(facts.amount_minor as numeric) / {{ minor_per_major }} as amount,
    facts.currency,
    facts.captured_total,
    cast(facts.captured_total as numeric) / {{ minor_per_major }} as captured_amount,
    facts.refunded_total,
    cast(facts.refunded_total as numeric) / {{ minor_per_major }} as refunded_amount,
    facts.reversed_total,
    cast(facts.reversed_total as numeric) / {{ minor_per_major }} as reversed_amount,
    facts.channel_id,
    facts.created_at,
    facts.created_at_local,
    facts.source_updated_at,
    -- witnessed milestones in fixed pipeline order, not chronological.
    -- 'failed' is terminal only: a failure on a payment that also succeeded
    -- stays out of the string.
    array_to_string(
        [
            'created',
            if(facts.routed_at is not null, 'routed', null),
            if(facts.threeds_started_at is not null, '3ds', null),
            if(facts.challenge_presented_at is not null, '3ds_challenged', null),
            if(facts.authenticated_at is not null, '3ds_authenticated', null),
            if(facts.challenge_abandoned_at is not null, '3ds_abandoned', null),
            if(facts.threeds_failed_at is not null, '3ds_failed', null),
            if(facts.gateway_called_at is not null, 'gateway_called', null),
            if(facts.authorized_at is not null, 'authorized', null),
            if(facts.captured_at is not null, 'captured', null),
            if(facts.extended_at is not null, 'extended', null),
            if(facts.voided_at is not null, 'voided', null),
            if(facts.reversed_at is not null, 'reversed', null),
            if(facts.refunded_at is not null, 'refunded', null),
            if(
                facts.failed_at is not null and facts.authorized_at is null,
                'failed',
                null
            )
        ],
        ' -> '
    ) as milestones,
    facts.routed_at,
    facts.threeds_started_at,
    facts.challenge_presented_at,
    facts.challenge_completed_at,
    facts.challenge_abandoned_at,
    facts.authenticated_at,
    facts.threeds_failed_at,
    facts.gateway_called_at,
    facts.authorized_at,
    facts.captured_at,
    facts.extended_at,
    facts.voided_at,
    facts.reversed_at,
    facts.refunded_at,
    facts.last_refunded_at,
    facts.failed_at,
    facts.gateway,
    facts.auth_transaction_id,
    facts.auth_initiated_by,
    facts.auth_external_op,
    facts.auth_verification,
    facts.routed_connector,
    facts.routed_provider_config,
    facts.routed_via_fallback,
    facts.n_routing_rows,
    facts.n_routing_not_completed,
    facts.decision_duration_ms,
    -- the net-auth-rate flag: the gateway produced, or errored producing, a
    -- verdict. This category list is the definition the summary's net rate
    -- follows.
    coalesce(
        facts.authorized_at is not null
        or facts.failure_category in (
            'gateway_declined',
            'pos_provider_declined',
            'gateway_no_verdict',
            'pos_no_verdict'
        ),
        false
    ) as gateway_reached,
    facts.failure_category,
    case facts.failure_category
        when 'validation' then 'validation'
        when 'pos_validation' then 'validation'
        when 'risk_error' then 'risk'
        when 'risk_rejected' then 'risk'
        when 'routing_error' then 'routing'
        when 'threeds_failed' then 'threeds'
        when 'threeds_expired_or_error' then 'threeds'
        when 'gateway_declined' then 'gateway_declined'
        when 'pos_provider_declined' then 'gateway_declined'
        when 'gateway_no_verdict' then 'gateway_error'
        when 'pos_no_verdict' then 'gateway_error'
        when 'pos_device_error' then 'device'
        when 'pos_cancelled' then 'cancelled'
    end as failure_stage,
    codes.decline_reason,
    facts.failure_reason,
    facts.failure_connector,
    facts.failure_response_code,
    facts.failure_response_message,
    facts.failure_http_status,
    facts.failure_outcome,
    facts.failure_initiated_by,
    facts.has_risk_result,
    facts.risk_allowed,
    facts.risk_requires_threeds,
    facts.has_routing_result,
    facts.has_threeds_result,
    facts.gateway_reference_id is not null as has_gateway_reference,
    facts.n_ops,
    facts.n_success_ops,
    facts.n_failure_ops,
    facts.n_pending_ops,
    facts.n_decision_failures,
    facts.n_capture_success,
    facts.n_unknown_op_types,
    facts.authorized_at is not null and facts.n_decision_failures > 0 as retried_auth,
    facts.n_refunds,
    facts.n_reversals,
    facts.n_threeds_attempts,
    facts.has_threeds,
    facts.threeds_status,
    facts.threeds_trans_status,
    facts.threeds_eci,
    facts.authentication_scheme,
    facts.threeds_authenticated,
    facts.threeds_challenge_mandated,
    facts.threeds_abandoned,
    facts.threeds_duration_s
from facts
left join
    {{ ref('decline_code_map') }} as codes
    on facts.failure_response_code = codes.connector_response_code
