"""
Verify the revenue calculation fix.

The fix: Use actual Stripe charge amounts instead of FunnelFox prices.
Match via customer + timestamp to link charges to subscriptions.
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

# Proposed fix: Link Stripe subscriptions to their first successful charge
run_query("""
-- Get first successful charge per subscription (via customer matching)
WITH stripe_subs AS (
    SELECT
        id AS stripe_sub_id,
        customer,
        status,
        created AS sub_created,
        metadata->>'ff_session_id' AS ff_session_id
    FROM raw_stripe.subscriptions
    WHERE metadata->>'ff_session_id' IS NOT NULL
      AND status IN ('active', 'trialing', 'past_due')
      AND created >= CURRENT_DATE - INTERVAL '30 days'
),
first_charge_per_customer AS (
    SELECT DISTINCT ON (customer)
        customer,
        id AS charge_id,
        amount / 100.0 AS charge_amount,
        created AS charge_created
    FROM raw_stripe.charges
    WHERE status = 'succeeded'
      AND description = 'Subscription creation'
      AND amount NOT IN (100, 200)
    ORDER BY customer, created ASC
)
SELECT
    'Fixed Attribution (Stripe revenue)' AS metric,
    COUNT(DISTINCT ss.stripe_sub_id) AS subscriptions,
    COUNT(DISTINCT fc.charge_id) AS matched_charges,
    ROUND(SUM(fc.charge_amount)::numeric, 2) AS actual_revenue_usd
FROM stripe_subs ss
LEFT JOIN first_charge_per_customer fc ON ss.customer = fc.customer

UNION ALL

SELECT
    'Current Attribution (FF price)' AS metric,
    COUNT(*) AS subscriptions,
    NULL::bigint AS matched_charges,
    ROUND(SUM(revenue_usd)::numeric, 2) AS actual_revenue_usd
FROM mart_marketing_attribution
WHERE first_session_date >= CURRENT_DATE - INTERVAL '30 days'
  AND converted = true

UNION ALL

SELECT
    'New Subscriptions (Stripe charges)' AS metric,
    COUNT(*) AS subscriptions,
    NULL::bigint AS matched_charges,
    ROUND(SUM(revenue_usd)::numeric, 2) AS actual_revenue_usd
FROM mart_new_subscriptions
WHERE subscription_date >= CURRENT_DATE - INTERVAL '30 days'
""", "1. COMPARISON: Current vs Fixed Revenue")

# Check if the numbers align better
run_query("""
-- Sessions that converted and have Stripe charges
WITH session_subs AS (
    SELECT
        s.id AS session_id,
        s.profile_id,
        ss.id AS stripe_sub_id,
        ss.customer,
        ss.created AS sub_created
    FROM raw_funnelfox.sessions s
    JOIN raw_stripe.subscriptions ss ON ss.metadata->>'ff_session_id' = s.id
    WHERE ss.status IN ('active', 'trialing', 'past_due')
      AND ss.created >= CURRENT_DATE - INTERVAL '30 days'
),
charges AS (
    SELECT
        customer,
        id AS charge_id,
        amount / 100.0 AS amount_usd,
        created AS charge_created
    FROM raw_stripe.charges
    WHERE status = 'succeeded'
      AND description = 'Subscription creation'
      AND amount NOT IN (100, 200)
)
SELECT
    COUNT(DISTINCT ss.session_id) AS sessions_with_sub,
    COUNT(DISTINCT c.charge_id) AS sessions_with_charge,
    COUNT(DISTINCT ss.profile_id) AS unique_profiles,
    ROUND(SUM(c.amount_usd)::numeric, 2) AS total_stripe_revenue
FROM session_subs ss
LEFT JOIN charges c ON ss.customer = c.customer
  AND c.charge_created >= ss.sub_created - INTERVAL '5 minutes'
  AND c.charge_created <= ss.sub_created + INTERVAL '5 minutes'
""", "2. Sessions with Subscriptions AND Charges")

conn.close()
print("\n\nVerification complete!")
