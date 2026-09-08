{{
    config(
        materialized="table",
        partition_by={"field": "created_at", "data_type": "timestamp", "granularity": "day"},
        cluster_by=["merchant_id"],
    )
}}

-- One row per hosted checkout session: the checkout-debugging table
-- product asked for (2026-09-06), published to Metabase through core.
-- Session facts from the staging allowlist, plus the merchant's display
-- name, the channel's name/type, an is_test_merchant flag and
-- effective_status.
--
-- HOW THIS RUNS: `table`, a FULL REBUILD from the staging views on every
-- dbt build, same as payments; reruns are idempotent.
--
-- TEST MERCHANTS ARE INCLUDED, flagged by is_test_merchant. This is the
-- opposite of the payments fact on purpose: product debugs checkout with
-- the test merchants, and hiding them would hide the sessions they are
-- looking for. Filter the flag out before counting anything.
--
-- STATUS IS NOT AN OUTCOME (nb 026): the service rewrites `status` only
-- when the pay page re-reads a Pending session, and nothing re-reads it
-- after the redirect, so most paid sessions stay Pending forever (1,643
-- of 3,886 on 2026-08-31). Expiry is computed at read time at source and
-- never written back. effective_status closes the second gap (Pending
-- past expires_on reads Expired; evaluated at build time, so a session
-- that expires after this morning's build reads Pending until tomorrow).
-- The first gap needs the payments join, which this table does not do:
-- there is no FK either way, the verified contract is
-- (merchant_id, order_id) = payments.(merchant_id, order_id) at order
-- grain, and a session's payment columns are the first follow-up if
-- product asks for them.
--
-- PII: none. Reads only the stg_ column allowlists; the shopper, order
-- and metadata blobs, the capability URL and the raw redirect URLs stop
-- at staging (see the stg_litecore__checkout_sessions header).
with
    sessions as (select * from {{ ref('stg_litecore__checkout_sessions') }}),

    test_merchants as (select merchant_id from {{ ref('test_merchants') }}),

    merchants as (select * from {{ ref('stg_litecore__business_entities') }}),

    channels as (select * from {{ ref('stg_litecore__channels') }})

-- Same major-unit rule as the payments fact: minor units per major unit
-- is 1000 for the three-decimal currencies, 100 for everything else.
{% set minor_per_major = "if(s.currency in ('BHD', 'IQD', 'JOD', 'KWD', 'LYD', 'OMR', 'TND'), 1000, 100)" %}

select
    s.checkout_session_id,
    s.merchant_id,
    m.merchant_name,
    t.merchant_id is not null as is_test_merchant,
    s.acting_business_account_id,
    s.order_id,
    s.status,
    case
        when s.status = 'Pending' and s.expires_on < current_timestamp() then 'Expired'
        else s.status
    end as effective_status,
    s.amount_minor,
    cast(s.amount_minor as numeric) / {{ minor_per_major }} as amount,
    s.currency,
    s.processing_type,
    s.three_ds_required,
    s.channel_id,
    c.channel_name,
    c.channel_type,
    c.channel_sub_type,
    s.redirect_success_host,
    s.redirect_failure_host,
    s.has_customer,
    s.has_metadata,
    s.has_order_data,
    s.idempotency_key_sha256,
    s.expires_on,
    s.expires_on_local,
    s.created_at,
    s.created_at_local,
    s.updated_at
from sessions as s
left join merchants as m on s.merchant_id = m.merchant_id
left join test_merchants as t on s.merchant_id = t.merchant_id
left join channels as c on s.channel_id = c.channel_id
