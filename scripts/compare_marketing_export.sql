-- =============================================================================
-- Marketing Performance Comparison Script
-- Date Range: 2026-01-16 to 2026-01-19
--
-- Purpose: Compare mart_marketing_performance with external export data
-- Usage: Run this query directly in your database client
-- =============================================================================

WITH aggregated AS (
    SELECT
        ad_name,

        -- Core metrics
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(installs) AS installs,
        SUM(registrations) AS registrations,
        SUM(purchases) AS ftd,
        SUM(spend_usd) AS spend_usd,

        -- Video metrics
        SUM(video_3_sec_plays) AS video_3_sec_plays,
        SUM(thru_plays) AS thru_plays,

        -- Funnel step rates (averaged across days)
        AVG(cr_1to2_screen) AS avg_cr_1to2_screen,
        AVG(cr_to_paywall) AS avg_cr_to_paywall,
        AVG(paywall_cvr) AS avg_paywall_cvr,
        AVG(upsell_cr) AS avg_upsell_cr,

        -- Attribution metrics
        SUM(attributed_sessions) AS attributed_sessions,
        SUM(attributed_conversions) AS attributed_conversions,
        SUM(revenue_usd) AS revenue_usd

    FROM analytics.mart_marketing_performance
    WHERE date BETWEEN '2026-01-16' AND '2026-01-19'
      AND ad_name IS NOT NULL
    GROUP BY ad_name
)

SELECT
    -- =========================================================================
    -- DIMENSIONS
    -- =========================================================================
    ad_name AS "Creo",

    -- =========================================================================
    -- CORE METRICS (matching your export columns)
    -- =========================================================================
    impressions AS "IMPRESS",
    clicks AS "CLICKS",
    installs AS "INSTALL",
    registrations AS "REG",
    ftd AS "FTD",
    ROUND(spend_usd::NUMERIC, 2) AS "SPEND",

    -- =========================================================================
    -- CALCULATED RATES
    -- =========================================================================
    CASE
        WHEN impressions > 0
        THEN ROUND((clicks::NUMERIC / impressions * 100), 2)
    END AS "CTR%",

    CASE
        WHEN clicks > 0
        THEN ROUND((spend_usd / clicks)::NUMERIC, 2)
    END AS "CPC",

    -- =========================================================================
    -- COST METRICS
    -- =========================================================================
    CASE
        WHEN installs > 0
        THEN ROUND((spend_usd / installs)::NUMERIC, 2)
    END AS "$Install",

    CASE
        WHEN registrations > 0
        THEN ROUND((spend_usd / registrations)::NUMERIC, 2)
    END AS "$REG",

    CASE
        WHEN ftd > 0
        THEN ROUND((spend_usd / ftd)::NUMERIC, 2)
    END AS "$FTD",

    -- =========================================================================
    -- CONVERSION RATES
    -- =========================================================================
    CASE
        WHEN clicks > 0
        THEN ROUND((installs::NUMERIC / clicks * 100), 2)
    END AS "Click2Install%",

    CASE
        WHEN installs > 0
        THEN ROUND((registrations::NUMERIC / installs * 100), 2)
    END AS "Inst2Reg%",

    CASE
        WHEN registrations > 0
        THEN ROUND((ftd::NUMERIC / registrations * 100), 2)
    END AS "R2D%",

    -- =========================================================================
    -- VIDEO METRICS
    -- =========================================================================
    CASE
        WHEN impressions > 0
        THEN ROUND((video_3_sec_plays::NUMERIC / impressions * 100), 2)
    END AS "Hook Rate%",

    CASE
        WHEN video_3_sec_plays > 0
        THEN ROUND((thru_plays::NUMERIC / video_3_sec_plays * 100), 2)
    END AS "Hold Rate%",

    -- =========================================================================
    -- FUNNEL STEP CONVERSION RATES (from Amplitude events)
    -- =========================================================================
    ROUND(avg_cr_1to2_screen::NUMERIC * 100, 2) AS "CR 1to2scr%",
    ROUND(avg_cr_to_paywall::NUMERIC * 100, 2) AS "CR to paywall%",
    ROUND(avg_paywall_cvr::NUMERIC * 100, 2) AS "pCVR%",
    ROUND(avg_upsell_cr::NUMERIC * 100, 2) AS "upsell CR%",

    -- =========================================================================
    -- REVENUE METRICS (from our attribution)
    -- =========================================================================
    attributed_sessions AS "Sessions",
    attributed_conversions AS "Conversions",
    ROUND(revenue_usd::NUMERIC, 2) AS "Revenue USD",
    CASE
        WHEN spend_usd > 0
        THEN ROUND((revenue_usd / spend_usd)::NUMERIC, 2)
    END AS "ROAS"

FROM aggregated
ORDER BY spend_usd DESC NULLS LAST;


-- =============================================================================
-- TOTALS ROW
-- =============================================================================
/*
SELECT
    'TOTAL' AS "Creo",
    SUM(impressions) AS "IMPRESS",
    SUM(clicks) AS "CLICKS",
    SUM(installs) AS "INSTALL",
    SUM(registrations) AS "REG",
    SUM(purchases) AS "FTD",
    ROUND(SUM(spend_usd)::NUMERIC, 2) AS "SPEND",
    ROUND((SUM(clicks)::NUMERIC / NULLIF(SUM(impressions), 0) * 100), 2) AS "CTR%",
    ROUND((SUM(spend_usd) / NULLIF(SUM(clicks), 0))::NUMERIC, 2) AS "CPC",
    ROUND((SUM(spend_usd) / NULLIF(SUM(installs), 0))::NUMERIC, 2) AS "$Install",
    ROUND((SUM(spend_usd) / NULLIF(SUM(registrations), 0))::NUMERIC, 2) AS "$REG",
    ROUND((SUM(spend_usd) / NULLIF(SUM(purchases), 0))::NUMERIC, 2) AS "$FTD",
    ROUND((SUM(video_3_sec_plays)::NUMERIC / NULLIF(SUM(impressions), 0) * 100), 2) AS "Hook Rate%",
    SUM(attributed_conversions) AS "Conversions",
    ROUND(SUM(revenue_usd)::NUMERIC, 2) AS "Revenue USD",
    ROUND((SUM(revenue_usd) / NULLIF(SUM(spend_usd), 0))::NUMERIC, 2) AS "ROAS"
FROM analytics.mart_marketing_performance
WHERE date BETWEEN '2026-01-16' AND '2026-01-19';
*/
