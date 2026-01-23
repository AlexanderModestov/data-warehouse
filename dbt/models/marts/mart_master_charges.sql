{{
    config(
        materialized='table'
    )
}}

/*
    Master Charges Mart

    Purpose: Single source of truth for all Stripe payment analytics
    Grain: One row per Stripe charge (charge_id)

    Data Sources:
    - raw_stripe.charges: Core payment data (includes embedded refund data)
    - raw_stripe.payment_intents: Intent-level data (status, cancellation)
    - raw_funnelfox.subscriptions: FunnelFox linkage (nullable)
    - raw_funnelfox.sessions: User session data (nullable)
    - raw_funnelfox.funnels: Funnel metadata (nullable)

    Business Logic:
    - Revenue in USD (amount / 100.0)
    - Exclude test payments ($1, $2)
    - FunnelFox linkage via subscriptions.psp_id = charges.id
    - Risk metrics from outcome JSON (EFW proxy approach)
    - Refund data from embedded fields on charges (refunded, amount_refunded)
    - Card BIN/issuer/product from payment_method_details JSON
*/

WITH stripe_charges AS (
    SELECT
        -- Core identifiers
        id AS charge_id,
        payment_intent AS payment_intent_id,
        customer AS customer_id,
        invoice AS invoice_id,

        -- Amounts
        amount / 100.0 AS amount_usd,
        currency,

        -- Status
        status,
        status = 'succeeded' AS is_successful,
        failure_code,

        -- Card info from payment_method_details JSON
        LOWER((payment_method_details::json->'card')->>'brand') AS card_brand,
        (payment_method_details::json->'card')->>'country' AS card_country,
        (payment_method_details::json->'card')->>'funding' AS card_funding,
        (payment_method_details::json->'card')->>'last4' AS card_last4,
        (payment_method_details::json->'card')->>'iin' AS card_bin,
        (payment_method_details::json->'card')->>'issuer' AS card_issuer,
        (payment_method_details::json->'card')->>'product' AS card_product,

        -- Customer billing country
        (billing_details::json->'address')->>'country' AS customer_country,

        -- Risk data from outcome JSON
        (outcome::json)->>'risk_level' AS risk_level,
        ((outcome::json)->>'risk_score')::INT AS risk_score,
        (outcome::json)->>'type' AS outcome_type,
        (outcome::json)->>'reason' AS outcome_reason,

        -- Dispute flag (embedded on charge)
        COALESCE(disputed, FALSE) AS is_disputed,
        dispute AS dispute_id,

        -- Embedded refund data (for fallback)
        COALESCE(refunded, FALSE) AS has_refund_embedded,
        COALESCE(amount_refunded, 0) / 100.0 AS refund_amount_embedded,

        -- Description (Subscription creation, Payment for invoice, etc.)
        description,

        -- Timestamps
        created AS created_at

    FROM {{ source('raw_stripe', 'charges') }}
    WHERE amount NOT IN (100, 200)  -- Exclude test payments ($1, $2)
),

-- Stripe invoices to link charges → subscriptions
stripe_invoices AS (
    SELECT
        id AS invoice_id,
        subscription AS stripe_subscription_id,
        charge AS invoice_charge_id
    FROM {{ source('raw_stripe', 'invoices') }}
    WHERE subscription IS NOT NULL
),

-- FunnelFox subscriptions for linkage
-- Note: psp_id contains Stripe subscription IDs (sub_*)
funnelfox_subscriptions AS (
    SELECT
        psp_id AS stripe_subscription_id,  -- Contains sub_* (Stripe subscription ID)
        id AS ff_subscription_id,
        profile_id,
        status AS ff_subscription_status,
        payment_provider,
        billing_interval,
        billing_interval_count,
        price / 100.0 AS subscription_price_usd
    FROM {{ source('raw_funnelfox', 'subscriptions') }}
    WHERE sandbox = false
),

-- First session per user for attribution
first_sessions AS (
    SELECT DISTINCT ON (profile_id)
        profile_id,
        id AS session_id,
        funnel_id,
        country,
        city,
        origin AS traffic_source,
        created_at AS first_session_at
    FROM {{ source('raw_funnelfox', 'sessions') }}
    ORDER BY profile_id, created_at ASC
),

-- Funnel metadata
funnels AS (
    SELECT
        id AS funnel_id,
        title AS funnel_title,
        type AS funnel_type,
        environment AS funnel_environment
    FROM {{ source('raw_funnelfox', 'funnels') }}
),

-- Subscription items to link subscriptions → products
subscription_items AS (
    SELECT DISTINCT ON (subscription)
        subscription AS stripe_subscription_id,
        (plan::json)->>'product' AS product_id
    FROM {{ source('raw_stripe', 'subscription_items') }}
    ORDER BY subscription, created DESC
),

-- Products catalog
products AS (
    SELECT
        id AS product_id,
        name AS product_name
    FROM {{ source('raw_stripe', 'products') }}
),

-- Payment intents for intent-level data
payment_intents AS (
    SELECT
        id AS intent_id,
        status AS intent_status,
        canceled_at AS intent_canceled_at,
        cancellation_reason AS intent_cancellation_reason,
        description AS intent_description,
        created AS intent_created_at
    FROM {{ source('raw_stripe', 'payment_intents') }}
),

-- Add failure categorization and derived risk flags
charges_with_categories AS (
    SELECT
        c.*,

        -- Failure categorization
        CASE
            WHEN c.status = 'succeeded' THEN NULL
            WHEN c.failure_code IN ('insufficient_funds', 'card_velocity_exceeded') THEN 'insufficient_funds'
            WHEN c.failure_code IN ('card_declined', 'generic_decline', 'do_not_honor') THEN 'card_declined'
            WHEN c.failure_code IN ('fraudulent', 'merchant_blacklist', 'stolen_card', 'lost_card') THEN 'fraud_block'
            WHEN c.failure_code IN ('authentication_required', 'card_not_supported') THEN 'authentication_required'
            WHEN c.failure_code = 'expired_card' THEN 'expired_card'
            WHEN c.failure_code IN ('invalid_card_number', 'invalid_cvc', 'invalid_expiry_month', 'invalid_expiry_year') THEN 'invalid_card'
            WHEN c.failure_code IN ('processing_error', 'try_again_later') THEN 'processing_error'
            ELSE 'technical_error'
        END AS failure_category,

        -- Recovery action
        CASE
            WHEN c.status = 'succeeded' THEN NULL
            WHEN c.failure_code IN ('insufficient_funds', 'card_velocity_exceeded', 'processing_error', 'try_again_later') THEN 'retry_eligible'
            WHEN c.failure_code IN ('card_declined', 'generic_decline', 'do_not_honor', 'expired_card', 'invalid_card_number', 'invalid_cvc', 'invalid_expiry_month', 'invalid_expiry_year') THEN 'request_new_card'
            WHEN c.failure_code IN ('authentication_required', 'card_not_supported') THEN 'verify_3ds'
            ELSE 'contact_support'
        END AS recovery_action,

        -- Derived risk flags (EFW proxy approach)
        c.outcome_type = 'blocked' AS is_blocked,
        c.failure_code IN ('fraudulent', 'merchant_blacklist', 'stolen_card', 'lost_card') AS is_fraud_decline

    FROM stripe_charges c
),

-- Intent-level stats for retry tracking
intent_stats AS (
    SELECT
        payment_intent_id,
        MAX(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END) AS intent_has_success
    FROM charges_with_categories
    WHERE payment_intent_id IS NOT NULL
    GROUP BY payment_intent_id
),

-- Add retry tracking info
charges_with_retry_info AS (
    SELECT
        c.*,
        ROW_NUMBER() OVER (PARTITION BY c.payment_intent_id ORDER BY c.created_at ASC) AS attempt_number,
        ROW_NUMBER() OVER (PARTITION BY c.payment_intent_id ORDER BY c.created_at ASC) = 1 AS is_first_attempt,
        ROW_NUMBER() OVER (PARTITION BY c.payment_intent_id ORDER BY c.created_at DESC) = 1 AS is_final_attempt,
        COALESCE(i.intent_has_success = 1, FALSE) AS intent_eventually_succeeded
    FROM charges_with_categories c
    LEFT JOIN intent_stats i ON c.payment_intent_id = i.payment_intent_id
)

-- Final output: join all sources
SELECT
    -- Core charge fields
    c.charge_id,
    c.payment_intent_id,
    c.customer_id,
    c.invoice_id,
    c.amount_usd,
    c.currency,
    c.status,
    c.is_successful,
    c.failure_code,
    c.failure_category,
    c.recovery_action,

    -- Card info
    c.card_brand,
    c.card_country,
    c.card_funding,
    c.card_last4,
    c.card_bin,
    c.card_issuer,
    c.card_product,
    c.customer_country,

    -- Risk fields
    c.risk_level,
    c.risk_score,
    c.outcome_type,
    c.outcome_reason,
    c.is_blocked,
    c.is_fraud_decline,

    -- Dispute fields
    c.is_disputed,
    c.dispute_id,

    -- Refund fields (from embedded data on charges)
    c.has_refund_embedded AS is_refunded,
    c.refund_amount_embedded AS refund_amount_usd,
    c.refund_amount_embedded >= c.amount_usd AS is_fully_refunded,
    CASE
        WHEN c.amount_usd > 0
        THEN c.refund_amount_embedded / c.amount_usd
        ELSE 0
    END AS refund_ratio,

    -- Payment intent fields
    pi.intent_status,
    pi.intent_canceled_at,
    pi.intent_cancellation_reason,
    pi.intent_description,
    pi.intent_created_at,

    -- FunnelFox linkage (nullable) - linked via invoice → Stripe subscription
    ff.profile_id,
    ff.ff_subscription_id,
    ff.ff_subscription_status,
    ff.payment_provider,
    ff.billing_interval,
    ff.billing_interval_count,
    ff.subscription_price_usd,

    -- Product info (from Stripe products catalog)
    p.product_name,

    -- Funnel & traffic info (nullable)
    f.funnel_id,
    f.funnel_title,
    f.funnel_type,
    f.funnel_environment,
    sess.country,
    sess.city,
    sess.traffic_source,
    sess.first_session_at,

    -- Time dimensions
    c.created_at,
    DATE(c.created_at AT TIME ZONE 'UTC') AS created_date,
    EXTRACT(HOUR FROM c.created_at AT TIME ZONE 'UTC')::INT AS hour_of_day,
    TRIM(TO_CHAR(c.created_at AT TIME ZONE 'UTC', 'Day')) AS day_of_week,
    DATE_TRUNC('week', c.created_at AT TIME ZONE 'UTC')::DATE AS week_start_date,
    DATE_TRUNC('month', c.created_at AT TIME ZONE 'UTC')::DATE AS month_start_date,

    -- Organic flag (no FunnelFox linkage)
    ff.ff_subscription_id IS NULL AS is_organic

FROM charges_with_categories c

-- Payment intent linkage
LEFT JOIN payment_intents pi
    ON pi.intent_id = c.payment_intent_id

-- Link charge → invoice → Stripe subscription
LEFT JOIN stripe_invoices inv
    ON inv.invoice_id = c.invoice_id

-- FunnelFox subscription linkage via Stripe subscription ID
LEFT JOIN funnelfox_subscriptions ff
    ON ff.stripe_subscription_id = inv.stripe_subscription_id

-- Subscription items to get product_id
LEFT JOIN subscription_items si
    ON si.stripe_subscription_id = inv.stripe_subscription_id

-- Products to get product_name
LEFT JOIN products p
    ON p.product_id = si.product_id

-- First session for attribution
LEFT JOIN first_sessions sess
    ON sess.profile_id = ff.profile_id

-- Funnel metadata
LEFT JOIN funnels f
    ON f.funnel_id = sess.funnel_id
