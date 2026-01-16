{{
    config(
        materialized='table'
    )
}}

/*
    Stripe Payments Daily Mart

    Purpose: Daily aggregated payment metrics for dashboards and trend analysis
    Grain: One row per day + funnel + card_country + card_brand + description

    Built on top of mart_stripe_payments for consistent business logic

    Description values from Stripe:
    - "Subscription creation" - New subscription
    - "Subscription update" - Subscription modification
    - etc.
*/

WITH payments AS (
    SELECT * FROM {{ ref('mart_stripe_payments') }}
),

-- Aggregate by date and dimensions
daily_metrics AS (
    SELECT
        created_date AS date,
        funnel_name,
        card_country,
        card_brand,
        description,

        -- Volume metrics
        COUNT(*) AS total_attempts,
        SUM(CASE WHEN is_successful THEN 1 ELSE 0 END) AS successful_payments,
        SUM(CASE WHEN NOT is_successful THEN 1 ELSE 0 END) AS failed_payments,

        -- Revenue metrics
        SUM(CASE WHEN is_successful THEN amount_usd ELSE 0 END) AS gross_revenue_usd,
        SUM(CASE WHEN NOT is_successful THEN amount_usd ELSE 0 END) AS failed_revenue_usd,

        -- Refund metrics
        SUM(CASE WHEN is_successful THEN refunded_amount_usd ELSE 0 END) AS refunded_usd,
        SUM(CASE WHEN has_refund THEN 1 ELSE 0 END) AS refund_count,
        SUM(CASE WHEN is_successful THEN net_revenue_usd ELSE 0 END) AS net_revenue_usd,

        -- Session recovery metrics
        SUM(CASE WHEN is_lost_payment THEN 1 ELSE 0 END) AS lost_payments,
        SUM(CASE WHEN is_lost_payment THEN amount_usd ELSE 0 END) AS lost_revenue_usd,
        SUM(CASE WHEN is_recovered_failure THEN 1 ELSE 0 END) AS recovered_payments,
        SUM(CASE WHEN is_recovered_failure THEN amount_usd ELSE 0 END) AS recovered_revenue_usd,

        -- Failure breakdown
        SUM(CASE WHEN failure_category = 'insufficient_funds' THEN 1 ELSE 0 END) AS failures_insufficient_funds,
        SUM(CASE WHEN failure_category = 'card_declined' THEN 1 ELSE 0 END) AS failures_card_declined,
        SUM(CASE WHEN failure_category = 'fraud_block' THEN 1 ELSE 0 END) AS failures_fraud_block,
        SUM(CASE WHEN failure_category = 'authentication_required' THEN 1 ELSE 0 END) AS failures_authentication_required,
        SUM(CASE WHEN failure_category IN ('expired_card', 'invalid_card', 'processing_error', 'technical_error') THEN 1 ELSE 0 END) AS failures_technical_error,
        SUM(CASE WHEN failure_category IS NOT NULL AND failure_category NOT IN ('insufficient_funds', 'card_declined', 'fraud_block', 'authentication_required', 'expired_card', 'invalid_card', 'processing_error', 'technical_error') THEN 1 ELSE 0 END) AS failures_other

    FROM payments
    GROUP BY
        created_date,
        funnel_name,
        card_country,
        card_brand,
        description
),

-- Calculate retry/recovery metrics (need to aggregate at intent level first)
intent_metrics AS (
    SELECT
        created_date,
        funnel_name,
        card_country,
        card_brand,
        description,
        payment_intent_id,
        COUNT(*) AS attempts_in_intent,
        MAX(CASE WHEN is_successful THEN 1 ELSE 0 END) AS intent_succeeded,
        MAX(CASE WHEN NOT is_successful THEN 1 ELSE 0 END) AS intent_had_failure
    FROM payments
    WHERE payment_intent_id IS NOT NULL
    GROUP BY
        created_date,
        funnel_name,
        card_country,
        card_brand,
        description,
        payment_intent_id
),

recovery_metrics AS (
    SELECT
        created_date AS date,
        funnel_name,
        card_country,
        card_brand,
        description,
        -- Intents with multiple attempts (had at least one retry)
        SUM(CASE WHEN attempts_in_intent > 1 THEN 1 ELSE 0 END) AS intents_with_retry,
        -- Intents that had a failure but eventually succeeded
        SUM(CASE WHEN intent_had_failure = 1 AND intent_succeeded = 1 THEN 1 ELSE 0 END) AS intents_recovered
    FROM intent_metrics
    GROUP BY
        created_date,
        funnel_name,
        card_country,
        card_brand,
        description
),

-- Combine all metrics
final AS (
    SELECT
        dm.date,
        dm.funnel_name,
        dm.card_country,
        dm.card_brand,
        dm.description,

        -- Volume
        dm.total_attempts,
        dm.successful_payments,
        dm.failed_payments,
        CASE
            WHEN dm.total_attempts > 0 THEN dm.successful_payments::NUMERIC / dm.total_attempts
            ELSE NULL
        END AS success_rate,

        -- Revenue
        dm.gross_revenue_usd,
        dm.failed_revenue_usd,

        -- Refund metrics
        dm.refunded_usd,
        dm.refund_count,
        dm.net_revenue_usd,
        CASE
            WHEN dm.gross_revenue_usd > 0 THEN dm.refunded_usd / dm.gross_revenue_usd
            ELSE NULL
        END AS refund_rate,

        -- Session recovery metrics
        dm.lost_payments,
        dm.lost_revenue_usd,
        dm.recovered_payments,
        dm.recovered_revenue_usd,

        -- Failure breakdown
        dm.failures_insufficient_funds,
        dm.failures_card_declined,
        dm.failures_fraud_block,
        dm.failures_authentication_required,
        dm.failures_technical_error,
        dm.failures_other,

        -- Intent-level recovery
        COALESCE(rm.intents_with_retry, 0) AS intents_with_retry,
        COALESCE(rm.intents_recovered, 0) AS intents_recovered,
        CASE
            WHEN COALESCE(rm.intents_with_retry, 0) > 0 THEN COALESCE(rm.intents_recovered, 0)::NUMERIC / rm.intents_with_retry
            ELSE NULL
        END AS recovery_rate

    FROM daily_metrics dm
    LEFT JOIN recovery_metrics rm
        ON dm.date = rm.date
        AND COALESCE(dm.funnel_name, '') = COALESCE(rm.funnel_name, '')
        AND COALESCE(dm.card_country, '') = COALESCE(rm.card_country, '')
        AND COALESCE(dm.card_brand, '') = COALESCE(rm.card_brand, '')
        AND COALESCE(dm.description, '') = COALESCE(rm.description, '')
)

SELECT * FROM final
