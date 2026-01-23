-- Diagnostic: Check revenue attribution in mart_marketing_attribution

-- 1. How many subscriptions have revenue_usd?
SELECT 
    'subscriptions_with_revenue' AS metric,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT stripe_subscription_id) AS unique_subscriptions,
    COUNT(DISTINCT CASE WHEN revenue_usd IS NOT NULL THEN stripe_subscription_id END) AS subs_with_revenue,
    SUM(CASE WHEN revenue_usd IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_revenue,
    ROUND(SUM(COALESCE(revenue_usd, 0))::numeric, 2) AS total_revenue
FROM mart_marketing_attribution
WHERE session_date >= CURRENT_DATE - INTERVAL '7 days';

-- 2. Check if facebook_campaign_id is populated
SELECT 
    'campaign_attribution' AS metric,
    COUNT(*) AS total_sessions,
    COUNT(DISTINCT CASE WHEN facebook_campaign_id IS NOT NULL THEN session_id END) AS sessions_with_campaign,
    COUNT(DISTINCT CASE WHEN revenue_usd IS NOT NULL THEN stripe_subscription_id END) AS subs_with_revenue,
    COUNT(DISTINCT CASE WHEN revenue_usd IS NOT NULL AND facebook_campaign_id IS NOT NULL THEN stripe_subscription_id END) AS subs_with_revenue_AND_campaign
FROM mart_marketing_attribution
WHERE session_date >= CURRENT_DATE - INTERVAL '7 days';

-- 3. Sample of subscriptions with revenue to see if they have campaign attribution
SELECT 
    stripe_subscription_id,
    session_date,
    revenue_usd,
    facebook_campaign_id,
    utm_source,
    utm_campaign,
    attribution_channel
FROM mart_marketing_attribution
WHERE session_date >= CURRENT_DATE - INTERVAL '7 days'
  AND stripe_subscription_id IS NOT NULL
ORDER BY session_date DESC
LIMIT 20;
