{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

/*
    New Subscriptions Mart

    Purpose: Track new subscriptions with revenue and acquisition context
    Grain: One row per new subscription (FunnelFox subscription record)

    Business Logic:
    - Revenue calculated as price / 100.0 (FunnelFox stores cents)
    - Exclude sandbox transactions
    - Links to sessions via timestamp correlation (session within 30 min before subscription)
    - Note: FunnelFox API doesn't expose profile_id on subscriptions, so we correlate by time

    Data Limitations:
    - Stripe charge linkage not currently possible (subscription IDs don't match)
    - Profile linkage is approximate based on session timing
*/

WITH funnelfox_subscriptions AS (
    SELECT
        id AS subscription_id,
        psp_id AS stripe_subscription_id,
        created_at AS subscription_created_at,
        status AS subscription_status,
        payment_provider,
        billing_interval,
        billing_interval_count,
        currency,
        price / 100.0 AS revenue_usd
    FROM {{ source('raw_funnelfox', 'subscriptions') }}
    WHERE sandbox = FALSE  -- Exclude test transactions
),

funnelfox_sessions AS (
    SELECT
        id AS session_id,
        profile_id,
        funnel_id,
        country,
        city,
        origin,
        created_at AS session_created_at
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

-- Match subscriptions to sessions via timestamp correlation
-- Find the most recent session within 30 minutes before subscription creation
subscriptions_with_sessions AS (
    SELECT
        s.*,
        sess.session_id,
        sess.profile_id,
        sess.funnel_id,
        sess.country,
        sess.city,
        sess.origin,
        sess.session_created_at,
        -- Minutes between session and subscription
        EXTRACT(EPOCH FROM (s.subscription_created_at - sess.session_created_at)) / 60.0 AS minutes_to_convert
    FROM funnelfox_subscriptions s
    LEFT JOIN LATERAL (
        SELECT *
        FROM funnelfox_sessions sess
        WHERE sess.session_created_at < s.subscription_created_at
          AND sess.session_created_at > s.subscription_created_at - INTERVAL '30 minutes'
        ORDER BY sess.session_created_at DESC
        FETCH FIRST 1 ROWS ONLY
    ) sess ON TRUE
),

final AS (
    SELECT
        -- Identifiers
        sws.subscription_id,
        sws.profile_id AS user_profile_id,
        sws.stripe_subscription_id,
        sws.session_id AS matched_session_id,

        -- Dates
        DATE(sws.subscription_created_at AT TIME ZONE 'UTC') AS subscription_date,
        sws.subscription_created_at AS subscription_timestamp,

        -- Revenue
        sws.revenue_usd,
        sws.currency,

        -- Subscription details
        sws.payment_provider,
        sws.billing_interval,
        sws.billing_interval_count,
        sws.subscription_status,

        -- Funnel context
        f.funnel_id,
        f.funnel_title,
        f.funnel_type,
        f.environment AS funnel_environment,

        -- Geography
        sws.country,
        sws.city,

        -- Acquisition
        sws.origin AS traffic_source,
        sws.session_created_at AS first_session_at,

        -- Time to convert (in hours)
        sws.minutes_to_convert / 60.0 AS hours_to_convert

    FROM subscriptions_with_sessions sws
    LEFT JOIN funnels f
        ON sws.funnel_id = f.funnel_id
)

SELECT * FROM final
