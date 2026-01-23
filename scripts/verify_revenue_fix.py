"""
Verify that revenue now matches between Marketing Performance and New Subscriptions tabs.
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
        print(" | ".join(f"{col:>25}" for col in columns))
        print("-" * (27 * len(columns)))

        # Print rows
        for row in rows:
            print(" | ".join(f"{str(v):>25}" for v in row))

        return rows

# 1. Compare revenue from all sources (last 30 days)
run_query("""
SELECT
    'mart_marketing_attribution (FIXED)' AS source,
    COUNT(*) AS subscriptions,
    ROUND(SUM(revenue_usd)::numeric, 2) AS total_revenue_usd
FROM mart_marketing_attribution
WHERE first_session_date >= CURRENT_DATE - INTERVAL '30 days'
  AND converted = true

UNION ALL

SELECT
    'mart_marketing_performance (SUM)' AS source,
    SUM(attributed_conversions)::int AS subscriptions,
    ROUND(SUM(revenue_usd)::numeric, 2) AS total_revenue_usd
FROM mart_marketing_performance
WHERE date >= CURRENT_DATE - INTERVAL '30 days'

UNION ALL

SELECT
    'mart_new_subscriptions' AS source,
    COUNT(*) AS subscriptions,
    ROUND(SUM(revenue_usd)::numeric, 2) AS total_revenue_usd
FROM mart_new_subscriptions
WHERE subscription_date >= CURRENT_DATE - INTERVAL '30 days'
""", "1. REVENUE COMPARISON - Should now be aligned!")

# 2. Check the difference
run_query("""
WITH marketing AS (
    SELECT SUM(revenue_usd) AS revenue
    FROM mart_marketing_attribution
    WHERE first_session_date >= CURRENT_DATE - INTERVAL '30 days'
      AND converted = true
),
subscriptions AS (
    SELECT SUM(revenue_usd) AS revenue
    FROM mart_new_subscriptions
    WHERE subscription_date >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT
    ROUND(m.revenue::numeric, 2) AS marketing_revenue,
    ROUND(s.revenue::numeric, 2) AS subscription_revenue,
    ROUND((s.revenue - m.revenue)::numeric, 2) AS difference,
    ROUND(((s.revenue - m.revenue) / s.revenue * 100)::numeric, 1) AS diff_percent
FROM marketing m, subscriptions s
""", "2. REVENUE DIFFERENCE ANALYSIS")

# 3. Verify revenue source is now Stripe charges
run_query("""
SELECT
    CASE WHEN stripe_charge_id IS NOT NULL THEN 'Has Stripe charge' ELSE 'No Stripe charge' END AS charge_status,
    COUNT(*) AS count,
    ROUND(SUM(revenue_usd)::numeric, 2) AS revenue_usd
FROM mart_marketing_attribution
WHERE first_session_date >= CURRENT_DATE - INTERVAL '30 days'
  AND converted = true
GROUP BY CASE WHEN stripe_charge_id IS NOT NULL THEN 'Has Stripe charge' ELSE 'No Stripe charge' END
""", "3. VERIFICATION: Revenue now linked to Stripe charges")

conn.close()
print("\n" + "="*80)
print("VERIFICATION COMPLETE!")
print("="*80)
print("\nExpected outcome:")
print("- Marketing Performance revenue should be LOWER than or EQUAL to New Subscriptions")
print("- The difference represents organic/unattributed subscriptions")
