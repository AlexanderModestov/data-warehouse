{{
    config(
        materialized='table'
    )
}}

/*
    Funnel Conversions Mart

    Purpose: Calculate web-to-subscription conversion rates and time-to-convert
    Grain: One row per funnel session (converted or not)

    3-Stage Conversion Funnel:
    1. attempted_payment - User initiated checkout (subscription created)
    2. started_trial     - User started a trial period (status = 'trialing')
    3. paid_successfully - User completed payment (status = 'active' + charge exists)

    Business Logic:
    - Start with all FunnelFox sessions
    - Link to Stripe subscriptions via ff_session_id metadata
    - Track conversion status, revenue, and time to convert
    - Exclude sandbox transactions and test subscriptions ($1, $2)

    Data Linkage:
    - FunnelFox sessions contain profile_id
    - Stripe subscriptions contain ff_session_id in metadata → links to sessions
    - FunnelFox subscriptions link to Stripe subscriptions via psp_id
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

-- Get successful charges (excluding test payments)
stripe_charges AS (
    SELECT
        id AS charge_id,
        customer,
        amount / 100.0 AS revenue_usd,
        currency,
        status,
        created AS charge_created_at
    FROM {{ source('raw_stripe', 'charges') }}
    WHERE status = 'succeeded'
      AND amount NOT IN (100, 200)  -- Exclude test payments ($1 and $2)
),

-- Link sessions to conversions through Stripe metadata
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

        -- Conversion data (only actual Stripe charges, not FunnelFox prices)
        chg.charge_id,
        chg.revenue_usd,
        chg.currency,
        chg.charge_created_at,
        ffs.ff_subscription_status,

        -- Conversion Types:
        -- 1. Subscription Activated - subscription created (any status)
        CASE
            WHEN ffs.ff_subscription_id IS NOT NULL THEN 1
            ELSE 0
        END AS subscription_activated,

        -- 2. Trial Started - subscription in trialing status
        CASE
            WHEN ffs.ff_subscription_status = 'trialing' THEN 1
            ELSE 0
        END AS trial_started,

        -- 3. Trial Converted - trialing subscription with successful charge
        CASE
            WHEN ffs.ff_subscription_status = 'trialing' AND chg.charge_id IS NOT NULL THEN 1
            ELSE 0
        END AS trial_converted,

        -- 4. Direct Paid - non-trial subscription with successful charge
        CASE
            WHEN ffs.ff_subscription_status != 'trialing' AND chg.charge_id IS NOT NULL THEN 1
            WHEN ffs.ff_subscription_status IS NULL AND chg.charge_id IS NOT NULL THEN 1
            ELSE 0
        END AS direct_paid,

        -- 5. Paid Successfully - any successful charge (regardless of status)
        CASE
            WHEN chg.charge_id IS NOT NULL THEN 1
            ELSE 0
        END AS paid_successfully,

        -- Legacy fields (kept for backwards compatibility)
        CASE
            WHEN ffs.ff_subscription_id IS NOT NULL THEN 1
            ELSE 0
        END AS converted,

        CASE
            WHEN chg.charge_id IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS had_purchase,

        -- Time to conversion in hours
        CASE
            WHEN ffs.subscription_created_at IS NOT NULL THEN
                EXTRACT(EPOCH FROM (ffs.subscription_created_at - sess.session_created_at)) / 3600.0
            ELSE NULL
        END AS time_to_conversion_hours

    FROM funnelfox_sessions sess
    LEFT JOIN stripe_subscriptions ss
        ON sess.session_id = ss.ff_session_id
    LEFT JOIN funnelfox_subscriptions ffs
        ON ss.stripe_subscription_id = ffs.stripe_subscription_id
    LEFT JOIN stripe_charges chg
        ON ss.customer = chg.customer
        AND chg.charge_created_at >= ss.created
        AND chg.charge_created_at < ss.created + INTERVAL '1 day'
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
        sc.subscription_activated,
        sc.trial_started,
        sc.trial_converted,
        sc.direct_paid,
        sc.paid_successfully,

        -- Legacy fields (kept for backwards compatibility)
        sc.converted,
        sc.had_purchase,
        sc.revenue_usd,
        sc.currency,
        sc.time_to_conversion_hours,

        -- Subscription status
        sc.ff_subscription_status AS subscription_status,

        -- Conversion timestamp
        sc.charge_created_at AS conversion_timestamp,
        DATE(sc.charge_created_at AT TIME ZONE 'UTC') AS conversion_date

    FROM session_conversions sc
    LEFT JOIN funnels f
        ON sc.funnel_id = f.funnel_id
)

SELECT * FROM final
