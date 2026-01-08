{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

/*
    Funnel Conversions Mart

    Purpose: Calculate web-to-subscription conversion rates and time-to-convert
    Grain: One row per funnel session (converted or not)

    Business Logic:
    - Start with all FunnelFox sessions
    - Left join to Stripe charges to identify conversions
    - Track conversion status, revenue, and time to convert
    - Exclude sandbox transactions
*/

WITH funnelfox_sessions AS (
    SELECT
        id AS session_id,
        profile_id,
        funnel_id,
        created_at AS session_created_at,
        country,
        city,
        origin,
        user_agent,
        ip,
        funnel_version
    FROM {{ source('raw_funnelfox', 'sessions') }}
),

funnels AS (
    SELECT
        id AS funnel_id,
        title AS funnel_title,
        type AS funnel_type,
        environment
    FROM {{ source('raw_funnelfox', 'funnels') }}
),

funnelfox_subscriptions AS (
    SELECT
        psp_id,
        profile_id,
        created_at AS subscription_created_at,
        sandbox
    FROM {{ source('raw_funnelfox', 'subscriptions') }}
    WHERE sandbox = FALSE  -- Exclude test transactions
),

stripe_charges AS (
    SELECT
        id AS charge_id,
        amount / 100.0 AS revenue_usd,
        currency,
        status,
        created AS charge_created_at,
        failure_code
    FROM {{ source('raw_stripe', 'charges') }}
    WHERE status = 'succeeded'
),

-- Link sessions to conversions
session_conversions AS (
    SELECT
        sess.session_id,
        sess.profile_id,
        sess.funnel_id,
        sess.session_created_at,
        sess.country,
        sess.city,
        sess.origin,
        sess.funnel_version,

        -- Conversion data
        chg.charge_id,
        chg.revenue_usd,
        chg.currency,
        chg.charge_created_at,

        -- Conversion flags and metrics
        CASE
            WHEN chg.charge_id IS NOT NULL THEN 1
            ELSE 0
        END AS converted,

        CASE
            WHEN chg.charge_id IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS had_purchase,

        -- Time to conversion in hours
        CASE
            WHEN chg.charge_created_at IS NOT NULL THEN
                EXTRACT(EPOCH FROM (chg.charge_created_at - sess.session_created_at)) / 3600.0
            ELSE NULL
        END AS time_to_conversion_hours

    FROM funnelfox_sessions sess
    LEFT JOIN funnelfox_subscriptions fsub
        ON sess.profile_id = fsub.profile_id
    LEFT JOIN stripe_charges chg
        ON fsub.psp_id = chg.charge_id
),

-- Add funnel metadata
final AS (
    SELECT
        -- Session identifiers
        sc.session_id,
        sc.profile_id,
        DATE(sc.session_created_at AT TIME ZONE 'UTC') AS session_date,
        sc.session_created_at AS session_timestamp,

        -- Funnel context
        sc.funnel_id,
        f.funnel_title,
        f.funnel_type,
        f.environment AS funnel_environment,
        sc.funnel_version,

        -- Geography
        sc.country,
        sc.city,

        -- Traffic source
        sc.origin AS traffic_source,

        -- Conversion metrics
        sc.converted,
        sc.had_purchase,
        sc.revenue_usd,
        sc.currency,
        sc.time_to_conversion_hours,

        -- Conversion timestamp
        sc.charge_created_at AS conversion_timestamp,
        DATE(sc.charge_created_at AT TIME ZONE 'UTC') AS conversion_date

    FROM session_conversions sc
    LEFT JOIN funnels f
        ON sc.funnel_id = f.funnel_id
)

SELECT * FROM final
