{{
    config(
        materialized='table'
    )
}}

/*
    Marketing Performance Mart

    Purpose: Comprehensive marketing performance metrics combining Facebook Ads
             statistics with attributed conversions, installs, and revenue for ROAS analysis.
    Grain: One row per date + campaign + adset + ad

    Data Sources:
    - Facebook Ads statistics (spend, impressions, clicks, video metrics)
    - Facebook campaign/adset/ad metadata
    - AppsFlyer installs (mobile app installs with attribution)
    - Attributed conversions from mart_marketing_attribution (real revenue)
    - Amplitude events (funnel step conversions)

    Key Metrics:
    - Spend metrics: spend_usd, impressions, clicks, CPM, CPC, CTR
    - Video metrics: hook_rate, hold_rate
    - Install metrics: installs, cost_per_install, click_to_install_rate
    - Funnel metrics: registrations, purchases, conversion rates
    - Funnel step metrics: screen1_to_screen2_rate, paywall_rate, upsell_rate
    - Revenue metrics: revenue_usd, ROAS, ARPU, CAC
*/

-- =============================================================================
-- 1. FACEBOOK AD STATISTICS (Daily metrics from FB)
-- =============================================================================
WITH facebook_stats AS (
    SELECT
        report_date AS date,
        facebook_campaign_id,
        facebook_adset_id,
        facebook_ad_id,
        campaign_name,
        adset_name,
        ad_name,

        -- Core metrics
        COALESCE(SUM(amount_spent), 0) AS spend_usd,
        COALESCE(SUM(impressions), 0) AS impressions,
        COALESCE(SUM(clicks), 0) AS clicks,
        COALESCE(SUM(unique_clicks), 0) AS unique_clicks,

        -- Video metrics
        COALESCE(SUM(video_3_sec_plays), 0) AS video_3_sec_plays,
        COALESCE(SUM(thru_plays), 0) AS thru_plays,

        -- FB-attributed conversions (from Facebook's attribution)
        COALESCE(SUM(registrations_completed), 0) AS fb_registrations,
        COALESCE(SUM(purchases), 0) AS fb_purchases,
        COALESCE(SUM(leads), 0) AS fb_leads

    FROM {{ ref('stg_facebook_new__ad_statistics') }}
    WHERE report_date IS NOT NULL
      AND facebook_ad_id IS NOT NULL
    GROUP BY
        report_date,
        facebook_campaign_id,
        facebook_adset_id,
        facebook_ad_id,
        campaign_name,
        adset_name,
        ad_name
),

-- =============================================================================
-- 2. APPSFLYER INSTALLS (Mobile app installs with FB attribution)
-- NOTE: AppsFlyer data not yet available. Uncomment when raw_appsflyer.installs exists.
-- =============================================================================
-- appsflyer_installs AS (
--     SELECT
--         DATE(install_time) AS date,
--         ad_id AS facebook_ad_id,
--         adset_id AS facebook_adset_id,
--         campaign_id AS facebook_campaign_id,
--
--         COUNT(*) AS installs,
--         SUM(COALESCE(cost_value, 0)) AS install_cost_usd
--
--     FROM {{ source('raw_appsflyer', 'installs') }}
--     WHERE media_source = 'Facebook Ads'
--       AND ad_id IS NOT NULL
--     GROUP BY
--         DATE(install_time),
--         ad_id,
--         adset_id,
--         campaign_id
-- ),

-- =============================================================================
-- 3. UNIQUE SUBSCRIPTION REVENUE (Each subscription counted exactly once)
-- Uses DISTINCT ON to pick first session's attribution for each subscription
-- NOTE: revenue_usd is now sourced from actual Stripe charges (not FunnelFox prices)
-- =============================================================================
unique_subscription_revenue AS (
    SELECT DISTINCT ON (stripe_subscription_id)
        stripe_subscription_id,
        session_date,
        facebook_campaign_id,
        revenue_usd  -- This is actual Stripe charge amount from mart_marketing_attribution
    FROM {{ ref('mart_marketing_attribution') }}
    WHERE stripe_subscription_id IS NOT NULL
      AND revenue_usd IS NOT NULL  -- Only include subscriptions with actual charges
    ORDER BY stripe_subscription_id, session_created_at ASC
),

-- Revenue aggregated by campaign/date (no joins = no fan-out)
revenue_by_campaign AS (
    SELECT
        session_date AS date,
        facebook_campaign_id,
        SUM(revenue_usd) AS attributed_revenue_usd,
        COUNT(*) AS unique_subscriptions
    FROM unique_subscription_revenue
    GROUP BY session_date, facebook_campaign_id
),

-- =============================================================================
-- 4. PROFILE METRICS (aggregated by date + campaign)
-- =============================================================================
attributed_conversions AS (
    SELECT
        session_date AS date,
        facebook_campaign_id,
        COUNT(*) AS profiles,
        COUNT(DISTINCT profile_id) AS unique_users,
        SUM(CASE WHEN revenue_usd IS NOT NULL THEN 1 ELSE 0 END) AS conversions
    FROM {{ ref('mart_marketing_attribution') }}
    WHERE facebook_campaign_id IS NOT NULL
    GROUP BY session_date, facebook_campaign_id
),

-- =============================================================================
-- 5. FUNNEL PER CAMPAIGN (dominant funnel based on session count)
-- =============================================================================
funnel_per_campaign AS (
    SELECT DISTINCT ON (facebook_campaign_id)
        facebook_campaign_id,
        funnel_id,
        funnel_title
    FROM (
        SELECT
            facebook_campaign_id,
            funnel_id,
            funnel_title,
            COUNT(*) AS session_count
        FROM {{ ref('mart_marketing_attribution') }}
        WHERE facebook_campaign_id IS NOT NULL
          AND funnel_id IS NOT NULL
        GROUP BY facebook_campaign_id, funnel_id, funnel_title
    ) sub
    ORDER BY facebook_campaign_id, session_count DESC
),

-- =============================================================================
-- 6. CAMPAIGN/ADSET/AD METADATA (Latest names)
-- =============================================================================
campaign_meta AS (
    SELECT DISTINCT ON (facebook_campaign_id)
        facebook_campaign_id,
        campaign_name,
        objective AS campaign_objective,
        status AS campaign_status
    FROM {{ ref('stg_facebook_new__campaigns') }}
    WHERE facebook_campaign_id IS NOT NULL
    ORDER BY facebook_campaign_id, created_time DESC
),

adset_meta AS (
    SELECT DISTINCT ON (facebook_adset_id)
        facebook_adset_id,
        adset_name,
        target_countries,
        status AS adset_status
    FROM {{ ref('stg_facebook_new__adsets') }}
    WHERE facebook_adset_id IS NOT NULL
    ORDER BY facebook_adset_id, created_time DESC
),

ad_meta AS (
    SELECT DISTINCT ON (facebook_ad_id)
        facebook_ad_id,
        ad_name,
        status AS ad_status
    FROM {{ ref('stg_facebook_new__ads') }}
    WHERE facebook_ad_id IS NOT NULL
    ORDER BY facebook_ad_id, created_time DESC
),

-- =============================================================================
-- 7. COMBINED METRICS
-- Includes both FB stats-based data AND revenue-only data for campaigns
-- that have attributed revenue but no recent FB stats (e.g., paused campaigns)
-- =============================================================================

-- First: Get all date/campaign combinations from revenue that are NOT in FB stats
revenue_only_campaigns AS (
    SELECT
        rc.date,
        rc.facebook_campaign_id,
        rc.attributed_revenue_usd,
        rc.unique_subscriptions
    FROM revenue_by_campaign rc
    WHERE rc.facebook_campaign_id IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM facebook_stats fs
          WHERE fs.date = rc.date
            AND fs.facebook_campaign_id = rc.facebook_campaign_id
      )
),

combined AS (
    -- Part 1: Rows from FB stats (with optional revenue)
    SELECT
        -- Dimensions
        fs.date,
        fs.facebook_campaign_id,
        fs.facebook_adset_id,
        fs.facebook_ad_id,

        -- Funnel (dominant funnel for this campaign)
        fpc.funnel_id,
        fpc.funnel_title,

        -- Names (prefer metadata, fallback to stats)
        COALESCE(cm.campaign_name, fs.campaign_name) AS campaign_name,
        COALESCE(am.adset_name, fs.adset_name) AS adset_name,
        COALESCE(adm.ad_name, fs.ad_name) AS ad_name,

        -- Campaign metadata
        cm.campaign_objective,
        cm.campaign_status,
        am.adset_status,
        adm.ad_status,
        am.target_countries,

        -- Core spend metrics
        COALESCE(fs.spend_usd, 0) AS spend_usd,
        COALESCE(fs.impressions, 0) AS impressions,
        COALESCE(fs.clicks, 0) AS clicks,
        COALESCE(fs.unique_clicks, 0) AS unique_clicks,

        -- Video metrics
        COALESCE(fs.video_3_sec_plays, 0) AS video_3_sec_plays,
        COALESCE(fs.thru_plays, 0) AS thru_plays,

        -- Install metrics (from AppsFlyer - not yet available)
        0 AS installs,
        0 AS install_cost_usd,

        -- FB-reported conversions
        COALESCE(fs.fb_registrations, 0) AS fb_registrations,
        COALESCE(fs.fb_purchases, 0) AS fb_purchases,
        COALESCE(fs.fb_leads, 0) AS fb_leads,

        -- Our attributed conversions (at campaign level from Amplitude)
        -- Only assign to first ad per campaign to prevent duplication when summing
        CASE
            WHEN ROW_NUMBER() OVER (
                PARTITION BY fs.date, fs.facebook_campaign_id
                ORDER BY fs.facebook_ad_id
            ) = 1 THEN COALESCE(ac.profiles, 0)
            ELSE 0
        END AS attributed_profiles,
        CASE
            WHEN ROW_NUMBER() OVER (
                PARTITION BY fs.date, fs.facebook_campaign_id
                ORDER BY fs.facebook_ad_id
            ) = 1 THEN COALESCE(ac.unique_users, 0)
            ELSE 0
        END AS attributed_users,
        CASE
            WHEN ROW_NUMBER() OVER (
                PARTITION BY fs.date, fs.facebook_campaign_id
                ORDER BY fs.facebook_ad_id
            ) = 1 THEN COALESCE(ac.conversions, 0)
            ELSE 0
        END AS attributed_conversions,
        -- Revenue from separate CTE (unique subscriptions only)
        -- Only assign to first ad per campaign to prevent duplication when summing
        CASE
            WHEN ROW_NUMBER() OVER (
                PARTITION BY fs.date, fs.facebook_campaign_id
                ORDER BY fs.facebook_ad_id
            ) = 1 THEN COALESCE(rc.attributed_revenue_usd, 0)
            ELSE 0
        END AS revenue_usd

    FROM facebook_stats fs

    LEFT JOIN campaign_meta cm
        ON fs.facebook_campaign_id = cm.facebook_campaign_id

    LEFT JOIN adset_meta am
        ON fs.facebook_adset_id = am.facebook_adset_id

    LEFT JOIN ad_meta adm
        ON fs.facebook_ad_id = adm.facebook_ad_id

    -- Funnel per campaign (dominant funnel)
    LEFT JOIN funnel_per_campaign fpc
        ON fs.facebook_campaign_id = fpc.facebook_campaign_id

    -- Profile metrics at campaign level
    LEFT JOIN attributed_conversions ac
        ON fs.date = ac.date
        AND fs.facebook_campaign_id = ac.facebook_campaign_id

    -- Revenue at campaign level (unique subscriptions, no duplication)
    LEFT JOIN revenue_by_campaign rc
        ON fs.date = rc.date
        AND fs.facebook_campaign_id = rc.facebook_campaign_id

    UNION ALL

    -- Part 2: Rows for campaigns with revenue but NO FB stats for that date
    -- (e.g., paused campaigns that still have conversions from earlier clicks)
    SELECT
        roc.date,
        roc.facebook_campaign_id,
        NULL AS facebook_adset_id,  -- No adset data available
        NULL AS facebook_ad_id,     -- No ad data available

        -- Funnel (dominant funnel for this campaign)
        fpc.funnel_id,
        fpc.funnel_title,

        -- Campaign metadata only (no adset/ad names)
        cm.campaign_name,
        NULL AS adset_name,
        NULL AS ad_name,

        cm.campaign_objective,
        cm.campaign_status,
        NULL AS adset_status,
        NULL AS ad_status,
        NULL AS target_countries,

        -- No spend metrics for paused campaigns
        0 AS spend_usd,
        0 AS impressions,
        0 AS clicks,
        0 AS unique_clicks,

        -- No video metrics
        0 AS video_3_sec_plays,
        0 AS thru_plays,

        -- No install metrics
        0 AS installs,
        0 AS install_cost_usd,

        -- No FB-reported conversions (campaign not active)
        0 AS fb_registrations,
        0 AS fb_purchases,
        0 AS fb_leads,

        -- Our attributed conversions
        COALESCE(ac.profiles, 0) AS attributed_profiles,
        COALESCE(ac.unique_users, 0) AS attributed_users,
        COALESCE(ac.conversions, 0) AS attributed_conversions,
        roc.attributed_revenue_usd AS revenue_usd

    FROM revenue_only_campaigns roc

    LEFT JOIN campaign_meta cm
        ON roc.facebook_campaign_id = cm.facebook_campaign_id

    LEFT JOIN funnel_per_campaign fpc
        ON roc.facebook_campaign_id = fpc.facebook_campaign_id

    LEFT JOIN attributed_conversions ac
        ON roc.date = ac.date
        AND roc.facebook_campaign_id = ac.facebook_campaign_id
),

-- =============================================================================
-- 8. FINAL WITH CALCULATED METRICS
-- =============================================================================
final AS (
    SELECT
        -- Dimensions
        date,
        facebook_campaign_id,
        facebook_adset_id,
        facebook_ad_id,
        funnel_id,
        funnel_title,
        campaign_name,
        adset_name,
        ad_name,
        campaign_objective,
        campaign_status,
        adset_status,
        ad_status,
        target_countries,

        -- Core spend metrics
        spend_usd,
        impressions,
        clicks,
        unique_clicks,

        -- Calculated spend metrics
        CASE
            WHEN impressions > 0 THEN (spend_usd / impressions) * 1000
            ELSE NULL
        END AS cpm,
        CASE
            WHEN clicks > 0 THEN spend_usd / clicks
            ELSE NULL
        END AS cpc,
        CASE
            WHEN impressions > 0 THEN clicks::NUMERIC / impressions
            ELSE NULL
        END AS ctr,

        -- Video metrics
        video_3_sec_plays,
        thru_plays,
        CASE
            WHEN impressions > 0 THEN video_3_sec_plays::NUMERIC / impressions
            ELSE NULL
        END AS hook_rate,
        CASE
            WHEN video_3_sec_plays > 0 THEN thru_plays::NUMERIC / video_3_sec_plays
            ELSE NULL
        END AS hold_rate,

        -- Install metrics (from AppsFlyer)
        installs,
        CASE
            WHEN installs > 0 THEN spend_usd / installs
            ELSE NULL
        END AS cost_per_install,
        CASE
            WHEN clicks > 0 THEN installs::NUMERIC / clicks
            ELSE NULL
        END AS click_to_install_rate,

        -- FB-reported conversions
        fb_registrations AS registrations,
        fb_purchases AS purchases,
        fb_leads AS leads,

        -- Funnel conversion rates (from FB attribution)
        CASE
            WHEN clicks > 0 THEN fb_registrations::NUMERIC / clicks
            ELSE NULL
        END AS clicks_to_regs_rate,
        CASE
            WHEN fb_registrations > 0 THEN fb_purchases::NUMERIC / fb_registrations
            ELSE NULL
        END AS regs_to_purchase_rate,
        CASE
            WHEN clicks > 0 THEN fb_purchases::NUMERIC / clicks
            ELSE NULL
        END AS clicks_to_purchase_rate,

        -- Install to Registration rate
        CASE
            WHEN installs > 0 THEN fb_registrations::NUMERIC / installs
            ELSE NULL
        END AS install_to_reg_rate,

        -- Registration to FTD (First Time Deposit/Purchase) rate
        CASE
            WHEN fb_registrations > 0 THEN fb_purchases::NUMERIC / fb_registrations
            ELSE NULL
        END AS reg_to_ftd_rate,

        -- Cost metrics
        CASE
            WHEN fb_registrations > 0 THEN spend_usd / fb_registrations
            ELSE NULL
        END AS cost_per_registration,
        CASE
            WHEN fb_purchases > 0 THEN spend_usd / fb_purchases
            ELSE NULL
        END AS cost_per_ftd,

        -- Funnel step conversion rates (from Amplitude events)
        -- NOTE: These require Amplitude event aggregation which is not yet implemented
        NULL::NUMERIC AS cr_1to2_screen,
        NULL::NUMERIC AS cr_to_paywall,
        NULL::NUMERIC AS paywall_cvr,
        NULL::NUMERIC AS upsell_cr,

        -- Our attributed metrics
        attributed_profiles,
        attributed_users,
        attributed_conversions,
        revenue_usd,

        -- Revenue metrics (using attributed revenue)
        CASE
            WHEN spend_usd > 0 THEN revenue_usd / spend_usd
            ELSE NULL
        END AS roas,
        CASE
            WHEN attributed_conversions > 0 THEN revenue_usd / attributed_conversions
            ELSE NULL
        END AS arpu,
        CASE
            WHEN attributed_conversions > 0 THEN spend_usd / attributed_conversions
            ELSE NULL
        END AS cac

    FROM combined
)

SELECT * FROM final
