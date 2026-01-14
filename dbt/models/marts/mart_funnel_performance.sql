{{
    config(
        materialized='table'
    )
}}

/*
    Funnel Performance Mart

    Purpose: Analyze funnel conversion rates by time period and traffic source
    Grain: One row per funnel + date + traffic source

    3-Stage Conversion Funnel:
    1. attempted_payment - User initiated checkout (subscription created)
    2. started_trial     - User started a trial period (status = 'trialing')
    3. paid_successfully - User completed payment (status = 'active' + charge exists)

    Business Logic:
    - Detect traffic source from user_agent (Facebook, Instagram, TikTok, organic)
    - Calculate conversion metrics and revenue
    - Include Amplitude engagement metrics for attribution analysis
    - Exclude sandbox transactions and test subscriptions ($1, $2)

    Data Linkage:
    - FunnelFox sessions contain profile_id
    - Stripe subscriptions contain ff_session_id in metadata → links to sessions
    - FunnelFox subscriptions link to Stripe subscriptions via psp_id
*/

WITH sessions_with_source AS (
    SELECT
        id AS session_id,
        profile_id,
        funnel_id,
        created_at AS session_created_at,
        DATE(created_at AT TIME ZONE 'UTC') AS session_date,
        country,
        city,
        origin,
        user_agent,
        -- Detect traffic source from user agent
        CASE
            WHEN user_agent LIKE '%FBAN/FBIOS%' OR user_agent LIKE '%FB_IAB/FB4A%' THEN 'facebook'
            WHEN user_agent LIKE '%Instagram%' THEN 'instagram'
            WHEN user_agent LIKE '%TikTok%' THEN 'tiktok'
            ELSE 'organic'
        END AS traffic_source
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

-- Stripe subscriptions with session linkage from metadata
stripe_subscriptions AS (
    SELECT
        id AS stripe_subscription_id,
        customer,
        status,
        created,
        metadata->>'ff_session_id' AS ff_session_id
    FROM {{ source('raw_stripe', 'subscriptions') }}
    WHERE metadata->>'ff_session_id' IS NOT NULL
),

-- FunnelFox subscriptions (link to Stripe via psp_id)
funnelfox_subscriptions AS (
    SELECT
        id AS ff_subscription_id,
        psp_id AS stripe_subscription_id,
        price_usd / 100.0 AS subscription_price_usd,
        sandbox,
        status AS ff_subscription_status,
        created_at AS subscription_created_at
    FROM {{ source('raw_funnelfox', 'subscriptions') }}
    WHERE sandbox = FALSE
      AND price NOT IN (100, 200)  -- Exclude test subscriptions ($1 and $2)
),

-- Get successful charges for subscriptions (excluding test payments)
stripe_charges AS (
    SELECT
        id AS charge_id,
        customer,
        amount / 100.0 AS revenue_usd,
        created AS charge_created_at,
        status
    FROM {{ source('raw_stripe', 'charges') }}
    WHERE status = 'succeeded'
      AND amount NOT IN (100, 200)  -- Exclude test payments ($1 and $2)
),

-- Link sessions to conversions through the Stripe metadata
session_conversions AS (
    SELECT
        s.session_id,
        s.profile_id,
        s.funnel_id,
        s.session_date,
        s.session_created_at,
        s.traffic_source,

        -- Conversion via Stripe subscription linked by ff_session_id
        ss.stripe_subscription_id,
        ffs.ff_subscription_id,
        ffs.ff_subscription_status,

        -- Get the first charge for this customer as the conversion charge (only actual Stripe charges)
        c.charge_id,
        c.revenue_usd,
        c.charge_created_at AS conversion_at,

        -- Conversion Types:
        -- 1. Subscription Activated - subscription created (any status)
        CASE WHEN ffs.ff_subscription_id IS NOT NULL THEN 1 ELSE 0 END AS subscription_activated,

        -- 2. Trial Started - subscription in trialing status
        CASE WHEN ffs.ff_subscription_status = 'trialing' THEN 1 ELSE 0 END AS trial_started,

        -- 3. Trial Converted - trialing subscription with successful charge
        CASE WHEN ffs.ff_subscription_status = 'trialing' AND c.charge_id IS NOT NULL THEN 1 ELSE 0 END AS trial_converted,

        -- 4. Direct Paid - non-trial subscription with successful charge
        CASE
            WHEN ffs.ff_subscription_status != 'trialing' AND c.charge_id IS NOT NULL THEN 1
            WHEN ffs.ff_subscription_status IS NULL AND c.charge_id IS NOT NULL THEN 1
            ELSE 0
        END AS direct_paid,

        -- 5. Paid Successfully - any successful charge (regardless of status)
        CASE WHEN c.charge_id IS NOT NULL THEN 1 ELSE 0 END AS paid_successfully,

        -- Legacy: converted (kept for backwards compatibility)
        CASE WHEN ffs.ff_subscription_id IS NOT NULL THEN 1 ELSE 0 END AS converted,

        CASE
            WHEN ffs.subscription_created_at IS NOT NULL THEN
                EXTRACT(EPOCH FROM (ffs.subscription_created_at - s.session_created_at)) / 3600.0
            ELSE NULL
        END AS hours_to_convert

    FROM sessions_with_source s
    LEFT JOIN stripe_subscriptions ss
        ON s.session_id = ss.ff_session_id
    LEFT JOIN funnelfox_subscriptions ffs
        ON ss.stripe_subscription_id = ffs.stripe_subscription_id
    LEFT JOIN stripe_charges c
        ON ss.customer = c.customer
        AND c.charge_created_at >= ss.created
        AND c.charge_created_at < ss.created + INTERVAL '1 day'
),

-- Amplitude engagement per user
amplitude_engagement AS (
    SELECT
        user_id AS profile_id,
        COUNT(*) AS event_count
    FROM {{ source('raw_amplitude', 'events') }}
    WHERE user_id IS NOT NULL
      AND user_id != ''
    GROUP BY user_id
),

-- Aggregate by funnel, date, traffic source
aggregated AS (
    SELECT
        sc.session_date AS date,
        sc.funnel_id,
        f.funnel_title,
        f.funnel_type,
        sc.traffic_source,

        -- Session metrics
        COUNT(DISTINCT sc.session_id) AS total_sessions,
        COUNT(DISTINCT sc.profile_id) AS unique_users,

        -- Conversion metrics
        SUM(sc.subscription_activated) AS subscriptions_activated,
        SUM(sc.trial_started) AS trials_started,
        SUM(sc.trial_converted) AS trials_converted,
        SUM(sc.direct_paid) AS direct_payments,
        SUM(sc.paid_successfully) AS paid_successfully,

        -- Conversion rates
        CASE
            WHEN COUNT(DISTINCT sc.session_id) > 0
            THEN SUM(sc.subscription_activated)::NUMERIC / COUNT(DISTINCT sc.session_id)
            ELSE 0
        END AS activation_rate,
        CASE
            WHEN COUNT(DISTINCT sc.session_id) > 0
            THEN SUM(sc.trial_started)::NUMERIC / COUNT(DISTINCT sc.session_id)
            ELSE 0
        END AS trial_rate,
        CASE
            WHEN COUNT(DISTINCT sc.session_id) > 0
            THEN SUM(sc.paid_successfully)::NUMERIC / COUNT(DISTINCT sc.session_id)
            ELSE 0
        END AS payment_rate,

        -- Legacy conversion metrics (kept for backwards compatibility)
        SUM(sc.converted) AS conversions,
        CASE
            WHEN COUNT(DISTINCT sc.session_id) > 0
            THEN SUM(sc.converted)::NUMERIC / COUNT(DISTINCT sc.session_id)
            ELSE 0
        END AS conversion_rate,
        COALESCE(SUM(CASE WHEN sc.paid_successfully = 1 THEN sc.revenue_usd ELSE 0 END), 0) AS revenue_usd,

        -- Time to convert
        AVG(sc.hours_to_convert) AS avg_hours_to_convert,

        -- Amplitude engagement
        COUNT(DISTINCT CASE WHEN ae.event_count > 0 THEN sc.profile_id END) AS users_with_amplitude_events,
        CASE
            WHEN COUNT(DISTINCT CASE WHEN ae.event_count > 0 THEN sc.profile_id END) > 0
            THEN SUM(COALESCE(ae.event_count, 0))::NUMERIC /
                 NULLIF(COUNT(DISTINCT CASE WHEN ae.event_count > 0 THEN sc.profile_id END), 0)
            ELSE 0
        END AS avg_events_per_user

    FROM session_conversions sc
    LEFT JOIN funnels f ON sc.funnel_id = f.funnel_id
    LEFT JOIN amplitude_engagement ae ON sc.profile_id = ae.profile_id
    GROUP BY
        sc.session_date,
        sc.funnel_id,
        f.funnel_title,
        f.funnel_type,
        sc.traffic_source
)

SELECT * FROM aggregated
