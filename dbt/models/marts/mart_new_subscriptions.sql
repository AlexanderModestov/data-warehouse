{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

/*
    New Subscriptions Mart

    Purpose: Track new subscriptions with revenue and acquisition context
    Grain: One row per new subscription (first successful charge)

    Business Logic:
    - Revenue calculated as amount / 100.0 (Stripe stores cents)
    - Only successful charges (status = 'succeeded')
    - Exclude sandbox transactions
    - Links FunnelFox sessions via profile_id for acquisition context
*/

WITH stripe_charges AS (
    SELECT
        id AS charge_id,
        amount / 100.0 AS revenue_usd,
        currency,
        status,
        created AS charge_created_at,
        customer AS customer_id,
        failure_code,
        invoice AS invoice_id
    FROM {{ source('raw_stripe', 'charges') }}
    WHERE status = 'succeeded'
),

funnelfox_subscriptions AS (
    SELECT
        id AS funnelfox_subscription_id,
        psp_id,
        profile_id,
        created_at AS subscription_created_at,
        status,
        sandbox,
        payment_provider,
        billing_interval,
        billing_interval_count,
        price / 100.0 AS price_usd
    FROM {{ source('raw_funnelfox', 'subscriptions') }}
    WHERE sandbox = FALSE  -- Exclude test transactions
),

funnelfox_sessions AS (
    SELECT
        profile_id,
        funnel_id,
        country,
        city,
        origin,
        created_at AS session_created_at,
        user_agent,
        ip
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

-- Join charges with FunnelFox subscriptions via psp_id
subscription_charges AS (
    SELECT
        sc.charge_id,
        sc.revenue_usd,
        sc.currency,
        sc.charge_created_at,
        sc.customer_id,
        sc.failure_code,
        fs.funnelfox_subscription_id,
        fs.profile_id,
        fs.subscription_created_at,
        fs.status AS subscription_status,
        fs.payment_provider,
        fs.billing_interval,
        fs.billing_interval_count,
        fs.price_usd AS subscription_price_usd,
        -- Use ROW_NUMBER to identify first charge per subscription
        ROW_NUMBER() OVER (
            PARTITION BY fs.funnelfox_subscription_id
            ORDER BY sc.charge_created_at ASC
        ) AS charge_sequence
    FROM stripe_charges sc
    INNER JOIN funnelfox_subscriptions fs
        ON sc.charge_id = fs.psp_id
),

-- Get only first charges (new subscriptions)
new_subscriptions AS (
    SELECT *
    FROM subscription_charges
    WHERE charge_sequence = 1
),

-- Add funnel context from sessions
final AS (
    SELECT
        -- Identifiers
        ns.charge_id AS subscription_id,
        ns.profile_id AS user_profile_id,
        ns.funnelfox_subscription_id,

        -- Dates
        DATE(ns.charge_created_at AT TIME ZONE 'UTC') AS subscription_date,
        ns.charge_created_at AS subscription_timestamp,

        -- Revenue
        ns.revenue_usd,
        ns.currency,
        ns.subscription_price_usd,

        -- Subscription details
        ns.payment_provider,
        ns.billing_interval,
        ns.billing_interval_count,
        ns.subscription_status,

        -- Funnel context
        f.funnel_id,
        f.funnel_title,
        f.funnel_type,
        f.environment AS funnel_environment,

        -- Geography
        sess.country,
        sess.city,

        -- Acquisition
        sess.origin AS traffic_source,
        sess.session_created_at AS first_session_at,

        -- Time to convert
        EXTRACT(EPOCH FROM (ns.charge_created_at - sess.session_created_at)) / 3600.0 AS hours_to_convert

    FROM new_subscriptions ns
    LEFT JOIN funnelfox_sessions sess
        ON ns.profile_id = sess.profile_id
    LEFT JOIN funnels f
        ON sess.funnel_id = f.funnel_id
)

SELECT * FROM final
