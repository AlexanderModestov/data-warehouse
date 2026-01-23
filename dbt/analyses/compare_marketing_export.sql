-- =============================================================================
-- Marketing Performance Comparison Script
-- Date Range: 2026-01-16 to 2026-01-19
--
-- Purpose: Compare mart_marketing_performance with external export data
-- Usage: Run this query and compare results with your spreadsheet export
-- =============================================================================

WITH aggregated AS (
    SELECT
        ad_name,

        -- Core metrics
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(installs) AS installs,  -- Will be 0 until AppsFlyer is connected
        SUM(registrations) AS registrations,
        SUM(purchases) AS ftd,
        SUM(spend_usd) AS spend_usd,

        -- Video metrics
        SUM(video_3_sec_plays) AS video_3_sec_plays,
        SUM(thru_plays) AS thru_plays,

        -- Funnel step events (raw counts for debugging)
        SUM(COALESCE((SELECT SUM(x) FROM unnest(ARRAY[0]) x), 0)) AS first_screen_raw,  -- placeholder

        -- Attribution metrics
        SUM(attributed_sessions) AS attributed_sessions,
        SUM(attributed_conversions) AS attributed_conversions,
        SUM(revenue_usd) AS revenue_usd

    FROM {{ ref('mart_marketing_performance') }}
    WHERE date BETWEEN '2026-01-16' AND '2026-01-19'
      AND ad_name IS NOT NULL
    GROUP BY ad_name
)

SELECT
    -- Dimensions
    ad_name AS "Creo",

    -- Core metrics (matching export columns)
    impressions AS "IMPRESS",
    clicks AS "CLICKS",
    installs AS "INSTALL",
    registrations AS "REG",
    ftd AS "FTD",
    ROUND(spend_usd::NUMERIC, 2) AS "SPEND",

    -- Calculated rates
    CASE
        WHEN impressions > 0
        THEN ROUND((clicks::NUMERIC / impressions * 100), 2)
    END AS "CTR%",

    CASE
        WHEN clicks > 0
        THEN ROUND((spend_usd / clicks)::NUMERIC, 2)
    END AS "CPC",

    -- Cost metrics
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

    -- Conversion rates
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

    -- Video metrics
    CASE
        WHEN impressions > 0
        THEN ROUND((video_3_sec_plays::NUMERIC / impressions * 100), 2)
    END AS "Hook Rate%",

    CASE
        WHEN video_3_sec_plays > 0
        THEN ROUND((thru_plays::NUMERIC / video_3_sec_plays * 100), 2)
    END AS "Hold Rate%"

FROM aggregated
ORDER BY spend_usd DESC NULLS LAST;
