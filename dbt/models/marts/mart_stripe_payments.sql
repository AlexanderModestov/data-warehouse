{{
    config(
        materialized='table',
        schema='analytics'
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
        -- Extract card details from payment_method_details JSON
        (payment_method_details::json->'card')->>'brand' AS card_brand,
        (payment_method_details::json->'card')->>'country' AS card_country,
        -- Extract billing country
        (billing_details::json->'address')->>'country' AS billing_country
    FROM {{ source('raw_stripe', 'charges') }}
),

-- Categorize failures with recovery actions
charges_with_failure_info AS (
    SELECT
        *,
        -- Failure category mapping
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
        -- Recovery action mapping
        CASE
            WHEN status = 'succeeded' THEN NULL
            WHEN failure_code IN ('insufficient_funds', 'card_velocity_exceeded', 'processing_error', 'try_again_later') THEN 'retry_eligible'
            WHEN failure_code IN ('card_declined', 'generic_decline', 'do_not_honor', 'expired_card', 'invalid_card_number', 'invalid_cvc', 'invalid_expiry_month', 'invalid_expiry_year') THEN 'request_new_card'
            WHEN failure_code IN ('authentication_required', 'card_not_supported') THEN 'verify_3ds'
            ELSE 'contact_support'
        END AS recovery_action
    FROM stripe_charges
),

-- Calculate retry attempt numbers and intent success
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
        -- Attempt number within payment intent
        ROW_NUMBER() OVER (
            PARTITION BY c.payment_intent_id
            ORDER BY c.created_at ASC
        ) AS attempt_number,
        -- Is first attempt
        CASE
            WHEN ROW_NUMBER() OVER (PARTITION BY c.payment_intent_id ORDER BY c.created_at ASC) = 1 THEN TRUE
            ELSE FALSE
        END AS is_first_attempt,
        -- Is final attempt
        CASE
            WHEN ROW_NUMBER() OVER (PARTITION BY c.payment_intent_id ORDER BY c.created_at DESC) = 1 THEN TRUE
            ELSE FALSE
        END AS is_final_attempt,
        -- Did this intent eventually succeed
        COALESCE(i.intent_has_success = 1, FALSE) AS intent_eventually_succeeded
    FROM charges_with_failure_info c
    LEFT JOIN intent_stats i ON c.payment_intent_id = i.payment_intent_id
),

-- Link to FunnelFox for funnel context
funnelfox_subscriptions AS (
    SELECT
        psp_id,
        profile_id
    FROM {{ source('raw_funnelfox', 'subscriptions') }}
    WHERE sandbox = FALSE
),

funnelfox_sessions AS (
    SELECT DISTINCT ON (profile_id)
        profile_id,
        funnel_id,
        origin AS traffic_source,
        country AS session_country
    FROM {{ source('raw_funnelfox', 'sessions') }}
    ORDER BY profile_id, created_at ASC
),

funnels AS (
    SELECT
        id AS funnel_id,
        title AS funnel_name
    FROM {{ source('raw_funnelfox', 'funnels') }}
),

-- Join to get profile_id and funnel context
charges_with_funnel AS (
    SELECT
        c.*,
        fsub.profile_id,
        sess.funnel_id,
        f.funnel_name,
        sess.traffic_source,
        sess.session_country
    FROM charges_with_retry_info c
    LEFT JOIN funnelfox_subscriptions fsub ON c.charge_id = fsub.psp_id
    LEFT JOIN funnelfox_sessions sess ON fsub.profile_id = sess.profile_id
    LEFT JOIN funnels f ON sess.funnel_id = f.funnel_id
),

-- Final output with all dimensions
final AS (
    SELECT
        -- Identifiers
        charge_id,
        payment_intent_id,
        customer_id,
        profile_id,

        -- Payment outcome
        status,
        CASE WHEN status = 'succeeded' THEN TRUE ELSE FALSE END AS is_successful,
        amount_usd,
        currency,

        -- Failure intelligence
        failure_code,
        failure_category,
        recovery_action,

        -- Retry tracking
        attempt_number,
        is_first_attempt,
        is_final_attempt,
        intent_eventually_succeeded,

        -- Time dimensions
        created_at,
        DATE(created_at AT TIME ZONE 'UTC') AS created_date,
        EXTRACT(HOUR FROM created_at AT TIME ZONE 'UTC')::INT AS hour_of_day,
        TRIM(TO_CHAR(created_at AT TIME ZONE 'UTC', 'Day')) AS day_of_week,
        DATE_TRUNC('week', created_at AT TIME ZONE 'UTC')::DATE AS week_start_date,
        DATE_TRUNC('month', created_at AT TIME ZONE 'UTC')::DATE AS month_start_date,

        -- Funnel dimensions
        funnel_name,
        traffic_source,
        NULL AS traffic_medium,  -- Not available in current data
        NULL AS traffic_campaign,  -- Not available in current data

        -- Geographic dimensions
        card_country,
        COALESCE(billing_country, session_country) AS customer_country,

        -- Card details
        card_brand

    FROM charges_with_funnel
)

SELECT * FROM final
