-- Rename/select only, deduped to the latest version per key. Grain:
-- threeds_id, one 3DS authentication attempt; a payment can have several.
--
-- The source mixes '' and NULL in the protocol columns — normalized to
-- NULL here so downstream reads one absence value.
with
    latest as (

        select
            id as threeds_id,
            payment_id,
            status,
            {{ column_or_null(source('litecore', 'payment_v2__threeds'), 'trans_status', 'string') }},
            {{ column_or_null(source('litecore', 'payment_v2__threeds'), 'trans_status_reason', 'string') }},
            {{ column_or_null(source('litecore', 'payment_v2__threeds'), 'eci', 'string') }},
            {{ column_or_null(source('litecore', 'payment_v2__threeds'), 'authentication_scheme', 'string') }},
            {{ column_or_null(source('litecore', 'payment_v2__threeds'), 'version', 'string') }},
            {{ column_or_null(source('litecore', 'payment_v2__threeds'), 'acs_challenge_mandated', 'string') }},
            {{ column_or_null(source('litecore', 'payment_v2__threeds'), 'instrument_id', 'string') }},
            {{ column_or_null(source('litecore', 'payment_v2__threeds'), 'acs_url', 'string') }},
            {{ column_or_null(source('litecore', 'payment_v2__threeds'), 'creq', 'string') }},
            created_at,
            updated_at

        from {{ source('litecore', 'payment_v2__threeds') }}
        qualify
            row_number() over (
                partition by id order by updated_at desc, _dlt_load_id desc
            )
            = 1

    )

select
    threeds_id,
    payment_id,
    status,
    nullif(trans_status, '') as trans_status,
    nullif(trans_status_reason, '') as trans_status_reason,
    nullif(eci, '') as eci,
    nullif(authentication_scheme, '') as authentication_scheme,
    nullif(version, '') as version,
    nullif(acs_challenge_mandated, '') as acs_challenge_mandated,
    instrument_id,
    -- presence only: a challenge means both the ACS URL and the CReq exist
    coalesce(acs_url, '') != '' and coalesce(creq, '') != '' as challenge_presented,
    created_at,
    updated_at
from latest
