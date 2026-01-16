{{
    config(
        materialized='table'
    )
}}

/*
    Stripe Payments Mart

    Purpose: Track all payment attempts with success/failure analysis and retry intelligence
    Grain: One row per payment attempt (Stripe charge)

    Business Logic:
    - Revenue calculated as amount / 100.0 (Stripe stores cents)
    - Failure codes categorized into actionable groups
    - Retry attempts linked via payment_intent
    - Recovery tracking shows if failed intents eventually succeeded
    - is_organic: TRUE when payment has no FunnelFox subscription (direct/organic conversion)
*/

WITH stripe_charges AS (
    SELECT
        id AS charge_id,
        payment_intent AS payment_intent_id,
        customer AS customer_id,
        amount / 100.0 AS amount_usd,
        currency,
        status,
        created AS created_at,
        failure_code,
        description,
        COALESCE(amount_refunded, 0) / 100.0 AS refunded_amount_usd,
        COALESCE(refunded, FALSE) AS has_refund,
        (payment_method_details::json->'card')->>'brand' AS card_brand,
        (payment_method_details::json->'card')->>'country' AS card_country,
        (billing_details::json->'address')->>'country' AS billing_country
    FROM {{ source('raw_stripe', 'charges') }}
    WHERE amount NOT IN (100, 200)  -- Exclude test payments ($1 and $2)
),

-- Stripe subscriptions with FunnelFox session linkage
stripe_subscriptions_with_session AS (
    SELECT
        customer,
        metadata->>'ff_session_id' AS ff_session_id,
        created AS subscription_created_at
    FROM {{ source('raw_stripe', 'subscriptions') }}
    WHERE metadata->>'ff_session_id' IS NOT NULL
),

-- Categorize failures with recovery actions
charges_with_failure_info AS (
    SELECT
        *,
        CASE
            WHEN status = 'succeeded' THEN NULL
            WHEN failure_code IN ('insufficient_funds', 'card_velocity_exceeded') THEN 'insufficient_funds'
            WHEN failure_code IN ('card_declined', 'generic_decline', 'do_not_honor') THEN 'card_declined'
            WHEN failure_code IN ('fraudulent', 'merchant_blacklist', 'stolen_card', 'lost_card') THEN 'fraud_block'
            WHEN failure_code IN ('authentication_required', 'card_not_supported') THEN 'authentication_required'
            WHEN failure_code = 'expired_card' THEN 'expired_card'
            WHEN failure_code IN ('invalid_card_number', 'invalid_cvc', 'invalid_expiry_month', 'invalid_expiry_year') THEN 'invalid_card'
            WHEN failure_code IN ('processing_error', 'try_again_later') THEN 'processing_error'
            ELSE 'technical_error'
        END AS failure_category,
        CASE
            WHEN status = 'succeeded' THEN NULL
            WHEN failure_code IN ('insufficient_funds', 'card_velocity_exceeded', 'processing_error', 'try_again_later') THEN 'retry_eligible'
            WHEN failure_code IN ('card_declined', 'generic_decline', 'do_not_honor', 'expired_card', 'invalid_card_number', 'invalid_cvc', 'invalid_expiry_month', 'invalid_expiry_year') THEN 'request_new_card'
            WHEN failure_code IN ('authentication_required', 'card_not_supported') THEN 'verify_3ds'
            ELSE 'contact_support'
        END AS recovery_action
    FROM stripe_charges
),

-- Calculate intent-level success for retry tracking
intent_stats AS (
    SELECT
        payment_intent_id,
        MAX(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END) AS intent_has_success
    FROM charges_with_failure_info
    WHERE payment_intent_id IS NOT NULL
    GROUP BY payment_intent_id
),

charges_with_retry_info AS (
    SELECT
        c.*,
        ROW_NUMBER() OVER (PARTITION BY c.payment_intent_id ORDER BY c.created_at ASC) AS attempt_number,
        ROW_NUMBER() OVER (PARTITION BY c.payment_intent_id ORDER BY c.created_at ASC) = 1 AS is_first_attempt,
        ROW_NUMBER() OVER (PARTITION BY c.payment_intent_id ORDER BY c.created_at DESC) = 1 AS is_final_attempt,
        COALESCE(i.intent_has_success = 1, FALSE) AS intent_eventually_succeeded
    FROM charges_with_failure_info c
    LEFT JOIN intent_stats i ON c.payment_intent_id = i.payment_intent_id
),

-- Session-level recovery: did any payment succeed in this session?
session_success AS (
    SELECT
        ss.ff_session_id,
        MAX(CASE WHEN c.status = 'succeeded' THEN 1 ELSE 0 END) AS session_has_success
    FROM charges_with_retry_info c
    INNER JOIN stripe_subscriptions_with_session ss
        ON c.customer_id = ss.customer
        AND c.created_at >= ss.subscription_created_at
        AND c.created_at < ss.subscription_created_at + INTERVAL '1 day'
    GROUP BY ss.ff_session_id
),

-- Link charges to sessions (may create duplicates, handled by DISTINCT ON below)
charges_with_session AS (
    SELECT
        c.*,
        ss.ff_session_id,
        COALESCE(sess.session_has_success, 0) AS session_has_success
    FROM charges_with_retry_info c
    LEFT JOIN stripe_subscriptions_with_session ss
        ON c.customer_id = ss.customer
        AND c.created_at >= ss.subscription_created_at
        AND c.created_at < ss.subscription_created_at + INTERVAL '1 day'
    LEFT JOIN session_success sess ON ss.ff_session_id = sess.ff_session_id
)

-- Final output: DISTINCT ON ensures one row per charge
SELECT DISTINCT ON (charge_id)
    charge_id,
    payment_intent_id,
    customer_id,
    NULL::VARCHAR AS profile_id,

    status,
    status = 'succeeded' AS is_successful,
    amount_usd,
    currency,

    refunded_amount_usd,
    has_refund,
    amount_usd - refunded_amount_usd AS net_revenue_usd,

    failure_code,
    failure_category,
    recovery_action,

    attempt_number,
    is_first_attempt,
    is_final_attempt,
    intent_eventually_succeeded,

    ff_session_id,
    session_has_success = 1 AS session_eventually_succeeded,
    status != 'succeeded' AND is_first_attempt AND session_has_success = 1 AS is_recovered_failure,
    status != 'succeeded' AND session_has_success = 0 AS is_lost_payment,

    created_at,
    DATE(created_at AT TIME ZONE 'UTC') AS created_date,
    EXTRACT(HOUR FROM created_at AT TIME ZONE 'UTC')::INT AS hour_of_day,
    TRIM(TO_CHAR(created_at AT TIME ZONE 'UTC', 'Day')) AS day_of_week,
    DATE_TRUNC('week', created_at AT TIME ZONE 'UTC')::DATE AS week_start_date,
    DATE_TRUNC('month', created_at AT TIME ZONE 'UTC')::DATE AS month_start_date,

    TRUE AS is_organic,  -- All organic since FunnelFox subscriptions not available
    description,

    NULL::VARCHAR AS funnel_name,
    NULL::VARCHAR AS traffic_source,
    NULL::VARCHAR AS traffic_medium,
    NULL::VARCHAR AS traffic_campaign,

    card_country,
    billing_country AS customer_country,
    card_brand

FROM charges_with_session
ORDER BY charge_id, created_at DESC
