{{
    config(
        materialized='table'
    )
}}

/*
    Stripe Risk Metrics Mart

    Purpose: Track fraud risk, chargebacks, and refund metrics for risk monitoring
    Grain: One row per day

    Data Sources:
    - mart_master_charges: Single source of truth for payment analytics
      (includes risk levels, fraud flags, disputes, refunds)

    Metrics calculated:
    - EFW (Early Fraud Warning) proxy: high risk + fraud declined
    - Chargeback counts/amounts by card network (Visa, Mastercard, Other)
    - Chargeback rates (disputes today / successful txns previous 30 days)
    - VAMP (Visa Acquirer Monitoring Program): EFW + Visa chargebacks
    - RDR (Rapid Dispute Resolution) - placeholder for future data
    - Refund metrics
*/

WITH charges AS (
    SELECT
        charge_id,
        created_date AS date,
        amount_usd,
        status,
        is_successful,
        card_brand,

        -- Risk fields (already extracted in mart_master_charges)
        risk_level,
        risk_score,
        is_blocked,
        is_fraud_decline,

        -- Dispute/Chargeback
        is_disputed,

        -- Refund fields
        is_refunded,
        refund_amount_usd

    FROM {{ ref('mart_master_charges') }}
),

-- Get all dates that have charges
daily_periods AS (
    SELECT DISTINCT date
    FROM charges
),

-- Daily refund metrics
refund_metrics AS (
    SELECT
        date,
        COUNT(*) FILTER (WHERE is_refunded) AS refund_count,
        COALESCE(SUM(refund_amount_usd), 0) AS refund_amount_usd,
        COUNT(*) FILTER (WHERE is_successful) AS successful_count,
        COALESCE(SUM(CASE WHEN is_successful THEN amount_usd ELSE 0 END), 0) AS successful_amount_usd
    FROM charges
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
        COALESCE(SUM(CASE WHEN risk_level IN ('elevated', 'highest') THEN amount_usd ELSE 0 END), 0) AS high_risk_amount_usd,

        -- Blocked for fraud (using pre-computed flags from mart_master_charges)
        COUNT(*) FILTER (WHERE is_blocked) AS blocked_count,
        COUNT(*) FILTER (WHERE is_fraud_decline) AS fraud_declined_count,
        COALESCE(SUM(CASE WHEN is_fraud_decline THEN amount_usd ELSE 0 END), 0) AS fraud_declined_amount_usd,

        -- Disputed transactions (chargebacks)
        COUNT(*) FILTER (WHERE is_disputed) AS disputed_count,
        COALESCE(SUM(CASE WHEN is_disputed THEN amount_usd ELSE 0 END), 0) AS disputed_amount_usd,

        -- Average risk score
        AVG(risk_score) FILTER (WHERE risk_score IS NOT NULL) AS avg_risk_score
    FROM charges
    GROUP BY 1
),

-- Chargeback metrics by card brand
chargeback_by_brand AS (
    SELECT
        date,
        COUNT(*) FILTER (WHERE is_disputed AND card_brand = 'mastercard') AS chb_count_mastercard,
        COALESCE(SUM(CASE WHEN is_disputed AND card_brand = 'mastercard' THEN amount_usd ELSE 0 END), 0) AS chb_amount_mastercard,
        COUNT(*) FILTER (WHERE is_disputed AND card_brand = 'visa') AS chb_count_visa,
        COALESCE(SUM(CASE WHEN is_disputed AND card_brand = 'visa' THEN amount_usd ELSE 0 END), 0) AS chb_amount_visa,
        COUNT(*) FILTER (WHERE is_disputed AND card_brand NOT IN ('mastercard', 'visa')) AS chb_count_other,
        COALESCE(SUM(CASE WHEN is_disputed AND card_brand NOT IN ('mastercard', 'visa') THEN amount_usd ELSE 0 END), 0) AS chb_amount_other
    FROM charges
    GROUP BY 1
),

-- Successful transactions by card brand for rate calculations
successful_by_brand AS (
    SELECT
        date,
        COUNT(*) FILTER (WHERE is_successful AND card_brand = 'mastercard') AS mc_successful_count,
        COUNT(*) FILTER (WHERE is_successful AND card_brand = 'visa') AS visa_successful_count,
        COUNT(*) FILTER (WHERE is_successful) AS total_successful_count
    FROM charges
    GROUP BY 1
),

-- Rolling 30-day successful transactions for chargeback rate calculations
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

        -- EFW (Early Fraud Warning) metrics
        -- Using proxy: high risk + fraud declined (actual EFW data not available)
        COALESCE(r.high_risk_count, 0) + COALESCE(r.fraud_declined_count, 0) AS efw_count,
        COALESCE(r.high_risk_amount_usd, 0) + COALESCE(r.fraud_declined_amount_usd, 0) AS efw_amount_usd,
        -- Alias for dashboard compatibility
        COALESCE(r.high_risk_count, 0) + COALESCE(r.fraud_declined_count, 0) AS efw_proxy_count,
        COALESCE(r.high_risk_amount_usd, 0) + COALESCE(r.fraud_declined_amount_usd, 0) AS efw_proxy_amount_usd,

        -- Chargebacks by card network
        COALESCE(cb.chb_count_mastercard, 0) AS chb_count_mastercard,
        COALESCE(cb.chb_amount_mastercard, 0) AS chb_amount_mastercard,
        COALESCE(cb.chb_count_visa, 0) AS chb_count_visa,
        COALESCE(cb.chb_amount_visa, 0) AS chb_amount_visa,
        COALESCE(cb.chb_count_other, 0) AS chb_count_other,
        COALESCE(cb.chb_amount_other, 0) AS chb_amount_other,

        -- Total chargebacks
        COALESCE(r.disputed_count, 0) AS total_chb_count,
        COALESCE(r.disputed_amount_usd, 0) AS total_chb_amount_usd,

        -- RDR (Rapid Dispute Resolution) - Visa disputes resolved through RDR
        -- Placeholder: actual RDR data would come from disputes table with specific status
        0 AS rdr_count,
        0.0 AS rdr_amount_usd,

        -- Chargeback rates (disputes today / successful txns previous 30 days)
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

        -- VAMP metrics (EFW + Visa chargebacks) - Visa Acquirer Monitoring Program
        COALESCE(r.high_risk_count, 0) + COALESCE(r.fraud_declined_count, 0) + COALESCE(cb.chb_count_visa, 0) AS vamp_count,
        CASE
            WHEN COALESCE(sr.visa_successful_count, 0) > 0
            THEN (COALESCE(r.high_risk_count, 0) + COALESCE(r.fraud_declined_count, 0) + COALESCE(cb.chb_count_visa, 0))::NUMERIC / sr.visa_successful_count
            ELSE NULL
        END AS vamp_rate,

        -- Refund metrics
        COALESCE(ref.refund_count, 0) AS refund_count,
        COALESCE(ref.refund_amount_usd, 0) AS refund_amount_usd,
        CASE
            WHEN COALESCE(ref.successful_amount_usd, 0) > 0
            THEN COALESCE(ref.refund_amount_usd, 0) / ref.successful_amount_usd
            ELSE NULL
        END AS refund_rate,

        -- Risk metrics (from Stripe Radar) - exposed for dashboard
        COALESCE(r.elevated_risk_count, 0) AS elevated_risk_count,
        COALESCE(r.highest_risk_count, 0) AS highest_risk_count,
        COALESCE(r.high_risk_count, 0) AS high_risk_count,
        COALESCE(r.high_risk_amount_usd, 0) AS high_risk_amount_usd,
        COALESCE(r.blocked_count, 0) AS blocked_count,
        COALESCE(r.fraud_declined_count, 0) AS fraud_declined_count,
        COALESCE(r.fraud_declined_amount_usd, 0) AS fraud_declined_amount_usd,
        COALESCE(r.avg_risk_score, 0) AS avg_risk_score,

        -- Successful transaction counts for reference
        COALESCE(sr.mc_successful_count, 0) AS mc_successful_count,
        COALESCE(sr.visa_successful_count, 0) AS visa_successful_count,
        COALESCE(sr.total_successful_count, 0) AS total_successful_count,
        COALESCE(ref.successful_amount_usd, 0) AS successful_amount_usd

    FROM daily_periods p
    LEFT JOIN refund_metrics ref ON p.date = ref.date
    LEFT JOIN risk_metrics r ON p.date = r.date
    LEFT JOIN chargeback_by_brand cb ON p.date = cb.date
    LEFT JOIN successful_rolling sr ON p.date = sr.date
)

SELECT * FROM final
ORDER BY date DESC
