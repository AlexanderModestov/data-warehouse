{{
    config(
        materialized='table'
    )
}}

/*
    Stripe Risk Metrics Mart

    Purpose: Track fraud risk, chargebacks, and refund metrics for risk monitoring
    Grain: One row per day

    Data Sources (all from charges table):
    - Refunds: amount_refunded, refunded fields
    - Risk/Fraud: outcome JSON field (risk_level, risk_score)
    - Disputes: disputed field + dispute JSON when available

    Metrics calculated:
    - Refund count and amount
    - High-risk transaction count (elevated/highest risk_level)
    - Fraud block count (payments blocked for fraud)
    - Disputed transaction count
    - Chargeback metrics by card network
*/

WITH daily_periods AS (
    SELECT DISTINCT
        DATE(created)::DATE AS date
    FROM {{ source('raw_stripe', 'charges') }}
    WHERE created >= '2024-01-01'
),

-- All charges with extracted risk and refund data
charges_with_risk AS (
    SELECT
        id AS charge_id,
        DATE(created)::DATE AS date,
        amount / 100.0 AS amount_usd,
        status,
        LOWER((payment_method_details::json->'card')->>'brand') AS card_brand,

        -- Refund data (embedded in charges)
        COALESCE(refunded, FALSE) AS is_refunded,
        COALESCE(amount_refunded, 0) / 100.0 AS refunded_amount_usd,

        -- Risk data from outcome JSON
        (outcome::json)->>'risk_level' AS risk_level,
        ((outcome::json)->>'risk_score')::INT AS risk_score,
        (outcome::json)->>'type' AS outcome_type,
        (outcome::json)->>'reason' AS outcome_reason,

        -- Dispute data (if available)
        COALESCE(disputed, FALSE) AS is_disputed,

        -- Failure code for fraud detection
        failure_code

    FROM {{ source('raw_stripe', 'charges') }}
    WHERE amount NOT IN (100, 200)  -- Exclude test payments
),

-- Daily refund metrics
refund_metrics AS (
    SELECT
        date,
        COUNT(*) FILTER (WHERE is_refunded) AS refund_count,
        SUM(refunded_amount_usd) AS refund_amount_usd,
        COUNT(*) FILTER (WHERE status = 'succeeded') AS successful_count,
        SUM(CASE WHEN status = 'succeeded' THEN amount_usd ELSE 0 END) AS successful_amount_usd
    FROM charges_with_risk
    GROUP BY 1
),

-- Daily risk metrics
risk_metrics AS (
    SELECT
        date,
        -- High risk transactions (Stripe Radar risk levels)
        COUNT(*) FILTER (WHERE risk_level = 'elevated') AS elevated_risk_count,
        COUNT(*) FILTER (WHERE risk_level = 'highest') AS highest_risk_count,
        COUNT(*) FILTER (WHERE risk_level IN ('elevated', 'highest')) AS high_risk_count,
        SUM(CASE WHEN risk_level IN ('elevated', 'highest') THEN amount_usd ELSE 0 END) AS high_risk_amount_usd,

        -- Blocked for fraud (outcome type = blocked or fraud failure codes)
        COUNT(*) FILTER (WHERE outcome_type = 'blocked') AS blocked_count,
        COUNT(*) FILTER (WHERE failure_code IN ('fraudulent', 'merchant_blacklist', 'stolen_card', 'lost_card')) AS fraud_declined_count,
        SUM(CASE WHEN failure_code IN ('fraudulent', 'merchant_blacklist', 'stolen_card', 'lost_card') THEN amount_usd ELSE 0 END) AS fraud_declined_amount_usd,

        -- Disputed transactions (chargebacks)
        COUNT(*) FILTER (WHERE is_disputed) AS disputed_count,
        SUM(CASE WHEN is_disputed THEN amount_usd ELSE 0 END) AS disputed_amount_usd,

        -- Average risk score
        AVG(risk_score) FILTER (WHERE risk_score IS NOT NULL) AS avg_risk_score
    FROM charges_with_risk
    GROUP BY 1
),

-- Chargeback metrics by card brand
chargeback_by_brand AS (
    SELECT
        date,
        COUNT(*) FILTER (WHERE is_disputed AND card_brand = 'mastercard') AS chb_count_mastercard,
        SUM(CASE WHEN is_disputed AND card_brand = 'mastercard' THEN amount_usd ELSE 0 END) AS chb_amount_mastercard,
        COUNT(*) FILTER (WHERE is_disputed AND card_brand = 'visa') AS chb_count_visa,
        SUM(CASE WHEN is_disputed AND card_brand = 'visa' THEN amount_usd ELSE 0 END) AS chb_amount_visa,
        COUNT(*) FILTER (WHERE is_disputed AND card_brand NOT IN ('mastercard', 'visa')) AS chb_count_other,
        SUM(CASE WHEN is_disputed AND card_brand NOT IN ('mastercard', 'visa') THEN amount_usd ELSE 0 END) AS chb_amount_other
    FROM charges_with_risk
    GROUP BY 1
),

-- Successful transactions by card brand for rate calculations
successful_by_brand AS (
    SELECT
        date,
        COUNT(*) FILTER (WHERE status = 'succeeded' AND card_brand = 'mastercard') AS mc_successful_count,
        COUNT(*) FILTER (WHERE status = 'succeeded' AND card_brand = 'visa') AS visa_successful_count,
        COUNT(*) FILTER (WHERE status = 'succeeded') AS total_successful_count
    FROM charges_with_risk
    GROUP BY 1
),

-- Rolling 30-day successful transactions for rate calculations
successful_rolling AS (
    SELECT
        date,
        mc_successful_count,
        visa_successful_count,
        total_successful_count,
        SUM(mc_successful_count) OVER (
            ORDER BY date
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS mc_prev_30d_successful,
        SUM(visa_successful_count) OVER (
            ORDER BY date
            ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
        ) AS visa_prev_30d_successful
    FROM successful_by_brand
),

-- Final metrics
final AS (
    SELECT
        p.date,

        -- Refund metrics
        COALESCE(ref.refund_count, 0) AS refund_count,
        COALESCE(ref.refund_amount_usd, 0) AS refund_amount_usd,
        COALESCE(ref.successful_amount_usd, 0) AS successful_amount_usd,

        -- Refund rate
        CASE
            WHEN COALESCE(ref.successful_amount_usd, 0) > 0
            THEN COALESCE(ref.refund_amount_usd, 0) / ref.successful_amount_usd
            ELSE NULL
        END AS refund_rate,

        -- Risk metrics (from Stripe Radar)
        COALESCE(r.elevated_risk_count, 0) AS elevated_risk_count,
        COALESCE(r.highest_risk_count, 0) AS highest_risk_count,
        COALESCE(r.high_risk_count, 0) AS high_risk_count,
        COALESCE(r.high_risk_amount_usd, 0) AS high_risk_amount_usd,

        -- Fraud blocks
        COALESCE(r.blocked_count, 0) AS blocked_count,
        COALESCE(r.fraud_declined_count, 0) AS fraud_declined_count,
        COALESCE(r.fraud_declined_amount_usd, 0) AS fraud_declined_amount_usd,

        -- EFW proxy: high risk + fraud declined (since we don't have EFW table)
        COALESCE(r.high_risk_count, 0) + COALESCE(r.fraud_declined_count, 0) AS efw_proxy_count,
        COALESCE(r.high_risk_amount_usd, 0) + COALESCE(r.fraud_declined_amount_usd, 0) AS efw_proxy_amount_usd,

        -- Disputed/Chargeback metrics
        COALESCE(r.disputed_count, 0) AS total_chb_count,
        COALESCE(r.disputed_amount_usd, 0) AS total_chb_amount_usd,

        -- Chargebacks by card network
        COALESCE(cb.chb_count_mastercard, 0) AS chb_count_mastercard,
        COALESCE(cb.chb_amount_mastercard, 0) AS chb_amount_mastercard,
        COALESCE(cb.chb_count_visa, 0) AS chb_count_visa,
        COALESCE(cb.chb_amount_visa, 0) AS chb_amount_visa,
        COALESCE(cb.chb_count_other, 0) AS chb_count_other,
        COALESCE(cb.chb_amount_other, 0) AS chb_amount_other,

        -- Chargeback rates
        CASE
            WHEN COALESCE(sr.mc_prev_30d_successful, 0) > 0
            THEN COALESCE(cb.chb_count_mastercard, 0)::NUMERIC / sr.mc_prev_30d_successful
            ELSE NULL
        END AS chb_rate_mastercard,
        CASE
            WHEN COALESCE(sr.visa_prev_30d_successful, 0) > 0
            THEN COALESCE(cb.chb_count_visa, 0)::NUMERIC / sr.visa_prev_30d_successful
            ELSE NULL
        END AS chb_rate_visa,

        -- VAMP metrics (EFW proxy + Visa chargebacks)
        COALESCE(r.high_risk_count, 0) + COALESCE(r.fraud_declined_count, 0) + COALESCE(cb.chb_count_visa, 0) AS vamp_count,
        CASE
            WHEN COALESCE(sr.visa_successful_count, 0) > 0
            THEN (COALESCE(r.high_risk_count, 0) + COALESCE(r.fraud_declined_count, 0) + COALESCE(cb.chb_count_visa, 0))::NUMERIC / sr.visa_successful_count
            ELSE NULL
        END AS vamp_rate,

        -- Average risk score
        r.avg_risk_score,

        -- Successful transaction counts for reference
        COALESCE(sr.mc_successful_count, 0) AS mc_successful_count,
        COALESCE(sr.visa_successful_count, 0) AS visa_successful_count,
        COALESCE(sr.total_successful_count, 0) AS total_successful_count

    FROM daily_periods p
    LEFT JOIN refund_metrics ref ON p.date = ref.date
    LEFT JOIN risk_metrics r ON p.date = r.date
    LEFT JOIN chargeback_by_brand cb ON p.date = cb.date
    LEFT JOIN successful_rolling sr ON p.date = sr.date
)

SELECT * FROM final
ORDER BY date DESC
