{{
    config(
        materialized='table'
    )
}}

/*
    New Subscriptions Mart

    Purpose: Track ALL new subscriptions with revenue and acquisition context
    Grain: One row per new subscription (Stripe charge with description = 'Subscription creation')

    Data Sources:
    1. Stripe charges - Primary source of truth for all payments
    2. Stripe subscriptions - Session linkage via metadata->>'ff_session_id'
    3. FunnelFox sessions/funnels - Funnel context when available
    4. FunnelFox subscriptions - Billing interval info (1 month, 3 month, etc.)

    Business Logic:
    - Revenue from Stripe charges (amount / 100.0)
    - Exclude test payments ($1, $2)
    - Billing interval from FunnelFox subscriptions via psp_id linkage
*/

WITH stripe_subscription_charges AS (
    SELECT
        id AS charge_id,
        customer AS customer_id,
        amount / 100.0 AS revenue_usd,
        currency,
        created AS subscription_timestamp,
        DATE(created AT TIME ZONE 'UTC') AS subscription_date,
        (payment_method_details::json->'card')->>'brand' AS card_brand,
        (payment_method_details::json->'card')->>'country' AS card_country
    FROM {{ source('raw_stripe', 'charges') }}
    WHERE status = 'succeeded'
      AND description = 'Subscription creation'
      AND amount NOT IN (100, 200)  -- Exclude test payments ($1, $2)
),

-- Stripe subscriptions with session linkage
stripe_subscriptions_with_session AS (
    SELECT
        customer,
        metadata->>'ff_session_id' AS ff_session_id,
        created AS stripe_subscription_created_at
    FROM {{ source('raw_stripe', 'subscriptions') }}
    WHERE metadata->>'ff_session_id' IS NOT NULL
),

-- FunnelFox sessions
funnelfox_sessions AS (
    SELECT
        id AS session_id,
        profile_id AS session_profile_id,
        funnel_id,
        country,
        city,
        origin,
        created_at AS session_created_at
    FROM {{ source('raw_funnelfox', 'sessions') }}
),

-- Funnels metadata
funnels AS (
    SELECT
        id AS funnel_id,
        title AS funnel_title,
        type AS funnel_type,
        environment AS funnel_environment
    FROM {{ source('raw_funnelfox', 'funnels') }}
),

-- FunnelFox subscriptions with billing info
funnelfox_subscriptions AS (
    SELECT
        id AS ff_subscription_id,
        psp_id,  -- Links to Stripe charge ID
        profile_id AS subscription_profile_id,
        status AS subscription_status,
        payment_provider,
        billing_interval,
        billing_interval_count,
        price / 100.0 AS subscription_price_usd  -- Convert cents to USD
    FROM {{ source('raw_funnelfox', 'subscriptions') }}
    WHERE sandbox = false
      AND psp_id IS NOT NULL
),

-- Join all sources (may create duplicates, handled by DISTINCT ON below)
subscriptions_joined AS (
    SELECT
        sc.charge_id,
        sc.customer_id,
        sc.subscription_date,
        sc.subscription_timestamp,
        sc.revenue_usd,
        sc.currency,
        sc.card_brand,
        sc.card_country,
        COALESCE(ffs.subscription_profile_id, sess.session_profile_id) AS session_profile_id,
        sess.country AS session_country,
        sess.city,
        sess.origin AS traffic_source,
        sess.session_created_at AS first_session_at,
        f.funnel_id,
        f.funnel_title,
        f.funnel_type,
        f.funnel_environment,
        -- FunnelFox subscription fields
        ffs.ff_subscription_id,
        ffs.subscription_status,
        ffs.payment_provider,
        ffs.billing_interval,
        ffs.billing_interval_count,
        ffs.subscription_price_usd
    FROM stripe_subscription_charges sc
    -- Join FunnelFox subscriptions via psp_id (Stripe charge ID)
    LEFT JOIN funnelfox_subscriptions ffs
        ON sc.charge_id = ffs.psp_id
    LEFT JOIN stripe_subscriptions_with_session ss
        ON sc.customer_id = ss.customer
        AND sc.subscription_timestamp >= ss.stripe_subscription_created_at
        AND sc.subscription_timestamp < ss.stripe_subscription_created_at + INTERVAL '1 hour'
    LEFT JOIN funnelfox_sessions sess
        ON ss.ff_session_id = sess.session_id
    LEFT JOIN funnels f
        ON sess.funnel_id = f.funnel_id
)

-- Final output: DISTINCT ON ensures one row per subscription
SELECT DISTINCT ON (charge_id)
    charge_id AS subscription_id,
    customer_id,
    session_profile_id AS user_profile_id,
    ff_subscription_id AS funnelfox_subscription_id,

    subscription_date,
    subscription_timestamp,
    revenue_usd,
    currency,
    subscription_price_usd,

    payment_provider,
    billing_interval,
    billing_interval_count,
    subscription_status,

    funnel_id,
    funnel_title,
    funnel_type,
    funnel_environment,

    COALESCE(session_country, card_country) AS country,
    city,
    traffic_source,
    first_session_at,

    CASE
        WHEN first_session_at IS NOT NULL
        THEN EXTRACT(EPOCH FROM (subscription_timestamp - first_session_at)) / 3600.0
        ELSE NULL
    END AS hours_to_convert,

    CASE
        WHEN billing_interval IS NOT NULL THEN 'recurring'
        ELSE 'one_time'
    END AS payment_type,
    ff_subscription_id IS NULL AS is_organic,
    card_brand,
    card_country

FROM subscriptions_joined
ORDER BY charge_id, subscription_timestamp DESC
