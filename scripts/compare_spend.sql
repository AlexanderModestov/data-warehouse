-- Diagnostic: Compare amount_spent from raw source vs spend_usd in mart
-- Run this in your database to find discrepancies

-- 1. Overall totals comparison
SELECT
    'raw_facebook.facebook_ad_statistics' AS source,
    SUM(amount_spent) AS total_amount_spent,
    COUNT(*) AS row_count,
    COUNT(DISTINCT facebook_ad_id) AS unique_ads
FROM raw_facebook.facebook_ad_statistics
WHERE report_date IS NOT NULL

UNION ALL

SELECT
    'mart_marketing_performance' AS source,
    SUM(spend_usd) AS total_amount_spent,
    COUNT(*) AS row_count,
    COUNT(DISTINCT facebook_ad_id) AS unique_ads
FROM analytics.mart_marketing_performance;

-- 2. Daily comparison to find where differences occur
WITH raw_daily AS (
    SELECT
        report_date AS date,
        SUM(amount_spent) AS raw_spend,
        COUNT(*) AS raw_rows
    FROM raw_facebook.facebook_ad_statistics
    WHERE report_date IS NOT NULL
    GROUP BY report_date
),
mart_daily AS (
    SELECT
        date,
        SUM(spend_usd) AS mart_spend,
        COUNT(*) AS mart_rows
    FROM analytics.mart_marketing_performance
    GROUP BY date
)
SELECT
    COALESCE(r.date, m.date) AS date,
    r.raw_spend,
    m.mart_spend,
    r.raw_spend - COALESCE(m.mart_spend, 0) AS difference,
    r.raw_rows,
    m.mart_rows
FROM raw_daily r
FULL OUTER JOIN mart_daily m ON r.date = m.date
WHERE ABS(COALESCE(r.raw_spend, 0) - COALESCE(m.mart_spend, 0)) > 0.01
ORDER BY date DESC;

-- 3. Check for rows excluded by filters (NULL facebook_ad_id)
SELECT
    'Rows with NULL facebook_ad_id' AS issue,
    COUNT(*) AS row_count,
    SUM(amount_spent) AS excluded_spend
FROM raw_facebook.facebook_ad_statistics
WHERE facebook_ad_id IS NULL;

-- 4. Check for rows excluded by filters (NULL report_date)
SELECT
    'Rows with NULL report_date' AS issue,
    COUNT(*) AS row_count,
    SUM(amount_spent) AS excluded_spend
FROM raw_facebook.facebook_ad_statistics
WHERE report_date IS NULL;

-- 5. Check for duplicate aggregation (same ad_id + date appearing multiple times)
SELECT
    report_date,
    facebook_ad_id,
    COUNT(*) AS occurrences,
    SUM(amount_spent) AS total_spend
FROM raw_facebook.facebook_ad_statistics
WHERE report_date IS NOT NULL
  AND facebook_ad_id IS NOT NULL
GROUP BY report_date, facebook_ad_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC
LIMIT 20;
