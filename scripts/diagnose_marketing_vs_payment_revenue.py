"""
Diagnose revenue discrepancy between Marketing Performance and Payment tabs.

Marketing Performance: $4,587.06 (from mart_marketing_attribution via FunnelFox prices)
Payment tab: $3,210.89 (from mart_new_subscriptions via Stripe charges)

Expected difference: $1,376.17
"""

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('PG_ANALYTICS_HOST'),
    database=os.getenv('PG_ANALYTICS_DBNAME'),
    user=os.getenv('PG_ANALYTICS_USER'),
    password=os.getenv('PG_ANALYTICS_PASSWORD'),
    port=os.getenv('PG_ANALYTICS_PORT', 5432)
)

cur = conn.cursor()
cur.execute("SET search_path TO analytics, raw_stripe, raw_funnelfox, raw_amplitude, raw_facebook, public")
cur.close()

def run_query(query, description):
    print(f"\n{'='*80}")
    print(f"{description}")
    print('='*80)
    with conn.cursor() as cur:
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

        # Print header
        print(" | ".join(f"{col:>20}" for col in columns))
        print("-" * (22 * len(columns)))

        # Print rows
        for row in rows:
            print(" | ".join(f"{str(v):>20}" for v in row))

        return rows

# 1. Compare total revenue from both sources (last 30 days)
run_query("""
SELECT
    'mart_marketing_attribution' AS source,
    COUNT(*) AS subscription_count,
    ROUND(SUM(revenue_usd)::numeric, 2) AS total_revenue_usd
FROM mart_marketing_attribution
WHERE first_session_date >= CURRENT_DATE - INTERVAL '30 days'
  AND converted = true

UNION ALL

SELECT
    'mart_new_subscriptions' AS source,
    COUNT(*) AS subscription_count,
    ROUND(SUM(revenue_usd)::numeric, 2) AS total_revenue_usd
FROM mart_new_subscriptions
WHERE subscription_date >= CURRENT_DATE - INTERVAL '30 days'
""", "1. Total Revenue Comparison (Last 30 Days)")

# 2. Check if FunnelFox prices differ from Stripe charges
run_query("""
WITH stripe_charges AS (
    SELECT
        id AS charge_id,
        amount / 100.0 AS stripe_amount,
        customer,
        created
    FROM raw_stripe.charges
    WHERE status = 'succeeded'
      AND description = 'Subscription creation'
      AND amount NOT IN (100, 200)
      AND created >= CURRENT_DATE - INTERVAL '30 days'
),
ff_subscriptions AS (
    SELECT
        psp_id,
        price / 100.0 AS ff_price,
        profile_id,
        created_at
    FROM raw_funnelfox.subscriptions
    WHERE sandbox = false
      AND price NOT IN (100, 200)
      AND created_at >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT
    COUNT(*) AS total_matches,
    SUM(CASE WHEN sc.stripe_amount = fs.ff_price THEN 1 ELSE 0 END) AS price_matches,
    SUM(CASE WHEN sc.stripe_amount <> fs.ff_price THEN 1 ELSE 0 END) AS price_mismatches,
    ROUND(SUM(sc.stripe_amount)::numeric, 2) AS total_stripe_amount,
    ROUND(SUM(fs.ff_price)::numeric, 2) AS total_ff_price,
    ROUND((SUM(fs.ff_price) - SUM(sc.stripe_amount))::numeric, 2) AS price_difference
FROM stripe_charges sc
JOIN ff_subscriptions fs ON sc.charge_id = fs.psp_id
""", "2. Price Match: Stripe Charges vs FunnelFox (by psp_id)")

# 3. Count subscriptions in each source
run_query("""
SELECT
    'Stripe succeeded charges (subscription creation)' AS source,
    COUNT(*) AS count,
    ROUND(SUM(amount / 100.0)::numeric, 2) AS total_usd
FROM raw_stripe.charges
WHERE status = 'succeeded'
  AND description = 'Subscription creation'
  AND amount NOT IN (100, 200)
  AND created >= CURRENT_DATE - INTERVAL '30 days'

UNION ALL

SELECT
    'FunnelFox subscriptions (non-sandbox)' AS source,
    COUNT(*) AS count,
    ROUND(SUM(price / 100.0)::numeric, 2) AS total_usd
FROM raw_funnelfox.subscriptions
WHERE sandbox = false
  AND price NOT IN (100, 200)
  AND created_at >= CURRENT_DATE - INTERVAL '30 days'

UNION ALL

SELECT
    'Stripe active subscriptions (linked to sessions)' AS source,
    COUNT(*) AS count,
    NULL::numeric AS total_usd
FROM raw_stripe.subscriptions
WHERE metadata->>'ff_session_id' IS NOT NULL
  AND status IN ('active', 'trialing', 'past_due')
  AND created >= CURRENT_DATE - INTERVAL '30 days'
""", "3. Raw Source Counts (Last 30 Days)")

# 4. Check mart_marketing_attribution revenue source
run_query("""
-- How does mart_marketing_attribution get revenue?
-- It uses: fs.price / 100.0 from FunnelFox subscriptions
-- Join path: sessions -> stripe_subscriptions (by ff_session_id) -> ff_subscriptions (by psp_id = stripe_sub_id)

WITH stripe_subs AS (
    SELECT
        id AS stripe_sub_id,
        metadata->>'ff_session_id' AS ff_session_id,
        status
    FROM raw_stripe.subscriptions
    WHERE metadata->>'ff_session_id' IS NOT NULL
      AND status IN ('active', 'trialing', 'past_due')
      AND created >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT
    COUNT(*) AS stripe_sub_count,
    COUNT(fs.id) AS matched_to_ff_sub,
    ROUND(SUM(fs.price / 100.0)::numeric, 2) AS ff_revenue_usd
FROM stripe_subs ss
LEFT JOIN raw_funnelfox.subscriptions fs
    ON ss.stripe_sub_id = fs.psp_id
    AND fs.sandbox = false
    AND fs.price NOT IN (100, 200)
""", "4. mart_marketing_attribution Join Path Analysis")

# 5. Check for FunnelFox subscriptions NOT in Stripe charges
run_query("""
WITH ff_subs AS (
    SELECT
        psp_id,
        price / 100.0 AS ff_price,
        profile_id
    FROM raw_funnelfox.subscriptions
    WHERE sandbox = false
      AND price NOT IN (100, 200)
      AND created_at >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT
    'FF subs with psp_id matching Stripe subscription (sub_...)' AS category,
    COUNT(*) AS count,
    ROUND(SUM(ff_price)::numeric, 2) AS revenue
FROM ff_subs
WHERE psp_id LIKE 'sub_%'

UNION ALL

SELECT
    'FF subs with psp_id matching Stripe charge (ch_...)' AS category,
    COUNT(*) AS count,
    ROUND(SUM(ff_price)::numeric, 2) AS revenue
FROM ff_subs
WHERE psp_id LIKE 'ch_%'

UNION ALL

SELECT
    'FF subs with psp_id matching payment_intent (pi_...)' AS category,
    COUNT(*) AS count,
    ROUND(SUM(ff_price)::numeric, 2) AS revenue
FROM ff_subs
WHERE psp_id LIKE 'pi_%'

UNION ALL

SELECT
    'FF subs with other psp_id format' AS category,
    COUNT(*) AS count,
    ROUND(SUM(ff_price)::numeric, 2) AS revenue
FROM ff_subs
WHERE psp_id NOT LIKE 'sub_%'
  AND psp_id NOT LIKE 'ch_%'
  AND psp_id NOT LIKE 'pi_%'
  AND psp_id IS NOT NULL
""", "5. FunnelFox psp_id Format Distribution")

# 6. Key question: Are there sessions linked to Stripe subscriptions WITHOUT actual charges?
run_query("""
WITH stripe_subs_with_sessions AS (
    SELECT
        s.id AS stripe_sub_id,
        s.metadata->>'ff_session_id' AS ff_session_id,
        s.customer,
        s.created AS sub_created
    FROM raw_stripe.subscriptions s
    WHERE s.metadata->>'ff_session_id' IS NOT NULL
      AND s.status IN ('active', 'trialing', 'past_due')
      AND s.created >= CURRENT_DATE - INTERVAL '30 days'
),
stripe_charges AS (
    SELECT
        customer,
        MIN(created) AS first_charge_created
    FROM raw_stripe.charges
    WHERE status = 'succeeded'
      AND description = 'Subscription creation'
      AND created >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY customer
)
SELECT
    'Stripe subscriptions with sessions' AS metric,
    COUNT(*) AS count
FROM stripe_subs_with_sessions

UNION ALL

SELECT
    'Stripe subs WITH matching charge (by customer)' AS metric,
    COUNT(*) AS count
FROM stripe_subs_with_sessions ss
JOIN stripe_charges sc ON ss.customer = sc.customer

UNION ALL

SELECT
    'Stripe subs WITHOUT matching charge' AS metric,
    COUNT(*) AS count
FROM stripe_subs_with_sessions ss
LEFT JOIN stripe_charges sc ON ss.customer = sc.customer
WHERE sc.customer IS NULL
""", "6. Stripe Subscriptions vs Actual Charges")

# 7. Revenue breakdown by billing interval
run_query("""
SELECT
    'mart_marketing_attribution' AS source,
    billing_interval,
    billing_interval_count,
    COUNT(*) AS count,
    ROUND(SUM(revenue_usd)::numeric, 2) AS revenue_usd
FROM mart_marketing_attribution
WHERE first_session_date >= CURRENT_DATE - INTERVAL '30 days'
  AND converted = true
GROUP BY billing_interval, billing_interval_count

UNION ALL

SELECT
    'mart_new_subscriptions' AS source,
    billing_interval,
    billing_interval_count,
    COUNT(*) AS count,
    ROUND(SUM(revenue_usd)::numeric, 2) AS revenue_usd
FROM mart_new_subscriptions
WHERE subscription_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY billing_interval, billing_interval_count
ORDER BY source, billing_interval, billing_interval_count
""", "7. Revenue by Billing Interval")

conn.close()
print("\n\nDiagnosis complete!")
