"""
Diagnose price differences between Stripe charges and FunnelFox subscription prices.
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

# 1. Match Stripe subscription charges via invoice -> subscription path
run_query("""
WITH stripe_subscription_charges AS (
    SELECT
        c.id AS charge_id,
        c.amount / 100.0 AS stripe_charge_amount,
        c.created AS charge_created,
        c.customer,
        i.subscription AS stripe_subscription_id
    FROM raw_stripe.charges c
    LEFT JOIN raw_stripe.invoices i ON c.invoice = i.id
    WHERE c.status = 'succeeded'
      AND c.description = 'Subscription creation'
      AND c.amount NOT IN (100, 200)
      AND c.created >= CURRENT_DATE - INTERVAL '30 days'
),
ff_subs AS (
    SELECT
        psp_id,
        price / 100.0 AS ff_price,
        profile_id,
        billing_interval,
        billing_interval_count
    FROM raw_funnelfox.subscriptions
    WHERE sandbox = false
      AND price NOT IN (100, 200)
)
SELECT
    COUNT(*) AS total_stripe_charges,
    COUNT(ff.psp_id) AS matched_to_ff,
    ROUND(SUM(sc.stripe_charge_amount)::numeric, 2) AS total_stripe_revenue,
    ROUND(SUM(ff.ff_price)::numeric, 2) AS total_ff_price,
    ROUND((SUM(ff.ff_price) - SUM(sc.stripe_charge_amount))::numeric, 2) AS price_difference
FROM stripe_subscription_charges sc
LEFT JOIN ff_subs ff ON sc.stripe_subscription_id = ff.psp_id
""", "1. Match via invoice->subscription path (correct linkage)")

# 2. Detailed breakdown of matched vs unmatched
run_query("""
WITH stripe_subscription_charges AS (
    SELECT
        c.id AS charge_id,
        c.amount / 100.0 AS stripe_charge_amount,
        c.created AS charge_created,
        c.customer,
        i.subscription AS stripe_subscription_id
    FROM raw_stripe.charges c
    LEFT JOIN raw_stripe.invoices i ON c.invoice = i.id
    WHERE c.status = 'succeeded'
      AND c.description = 'Subscription creation'
      AND c.amount NOT IN (100, 200)
      AND c.created >= CURRENT_DATE - INTERVAL '30 days'
),
ff_subs AS (
    SELECT
        psp_id,
        price / 100.0 AS ff_price,
        profile_id,
        billing_interval,
        billing_interval_count
    FROM raw_funnelfox.subscriptions
    WHERE sandbox = false
      AND price NOT IN (100, 200)
)
SELECT
    CASE
        WHEN ff.psp_id IS NOT NULL THEN 'Matched to FunnelFox'
        ELSE 'No FunnelFox match'
    END AS match_status,
    COUNT(*) AS count,
    ROUND(SUM(sc.stripe_charge_amount)::numeric, 2) AS stripe_revenue,
    ROUND(SUM(ff.ff_price)::numeric, 2) AS ff_price
FROM stripe_subscription_charges sc
LEFT JOIN ff_subs ff ON sc.stripe_subscription_id = ff.psp_id
GROUP BY CASE WHEN ff.psp_id IS NOT NULL THEN 'Matched to FunnelFox' ELSE 'No FunnelFox match' END
""", "2. Matched vs Unmatched Stripe Charges")

# 3. Sample of price differences
run_query("""
WITH stripe_subscription_charges AS (
    SELECT
        c.id AS charge_id,
        c.amount / 100.0 AS stripe_charge_amount,
        c.created AS charge_created,
        c.customer,
        i.subscription AS stripe_subscription_id
    FROM raw_stripe.charges c
    LEFT JOIN raw_stripe.invoices i ON c.invoice = i.id
    WHERE c.status = 'succeeded'
      AND c.description = 'Subscription creation'
      AND c.amount NOT IN (100, 200)
      AND c.created >= CURRENT_DATE - INTERVAL '30 days'
),
ff_subs AS (
    SELECT
        psp_id,
        price / 100.0 AS ff_price,
        profile_id,
        billing_interval,
        billing_interval_count
    FROM raw_funnelfox.subscriptions
    WHERE sandbox = false
      AND price NOT IN (100, 200)
)
SELECT
    sc.charge_id,
    sc.stripe_charge_amount,
    ff.ff_price,
    (ff.ff_price - sc.stripe_charge_amount) AS diff,
    ff.billing_interval,
    ff.billing_interval_count
FROM stripe_subscription_charges sc
JOIN ff_subs ff ON sc.stripe_subscription_id = ff.psp_id
WHERE ff.ff_price <> sc.stripe_charge_amount
LIMIT 10
""", "3. Sample of Price Differences (Stripe vs FunnelFox)")

# 4. What's in FunnelFox but NOT in Stripe charges?
run_query("""
WITH stripe_subscription_ids AS (
    SELECT DISTINCT i.subscription AS stripe_subscription_id
    FROM raw_stripe.charges c
    JOIN raw_stripe.invoices i ON c.invoice = i.id
    WHERE c.status = 'succeeded'
      AND c.description = 'Subscription creation'
      AND c.amount NOT IN (100, 200)
      AND c.created >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT
    'FunnelFox subs in last 30 days' AS category,
    COUNT(*) AS count,
    ROUND(SUM(price / 100.0)::numeric, 2) AS total_price
FROM raw_funnelfox.subscriptions
WHERE sandbox = false
  AND price NOT IN (100, 200)
  AND created_at >= CURRENT_DATE - INTERVAL '30 days'

UNION ALL

SELECT
    'FF subs WITH matching Stripe charge' AS category,
    COUNT(*) AS count,
    ROUND(SUM(fs.price / 100.0)::numeric, 2) AS total_price
FROM raw_funnelfox.subscriptions fs
JOIN stripe_subscription_ids si ON fs.psp_id = si.stripe_subscription_id
WHERE fs.sandbox = false
  AND fs.price NOT IN (100, 200)
  AND fs.created_at >= CURRENT_DATE - INTERVAL '30 days'

UNION ALL

SELECT
    'FF subs WITHOUT matching Stripe charge' AS category,
    COUNT(*) AS count,
    ROUND(SUM(fs.price / 100.0)::numeric, 2) AS total_price
FROM raw_funnelfox.subscriptions fs
LEFT JOIN stripe_subscription_ids si ON fs.psp_id = si.stripe_subscription_id
WHERE fs.sandbox = false
  AND fs.price NOT IN (100, 200)
  AND fs.created_at >= CURRENT_DATE - INTERVAL '30 days'
  AND si.stripe_subscription_id IS NULL
""", "4. FunnelFox Subscriptions Without Stripe Charges")

# 5. What are the FF subs without Stripe charges?
run_query("""
WITH stripe_subscription_ids AS (
    SELECT DISTINCT i.subscription AS stripe_subscription_id
    FROM raw_stripe.charges c
    JOIN raw_stripe.invoices i ON c.invoice = i.id
    WHERE c.status = 'succeeded'
      AND c.description = 'Subscription creation'
      AND c.created >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT
    fs.status,
    fs.payment_provider,
    COUNT(*) AS count,
    ROUND(SUM(fs.price / 100.0)::numeric, 2) AS total_price
FROM raw_funnelfox.subscriptions fs
LEFT JOIN stripe_subscription_ids si ON fs.psp_id = si.stripe_subscription_id
WHERE fs.sandbox = false
  AND fs.price NOT IN (100, 200)
  AND fs.created_at >= CURRENT_DATE - INTERVAL '30 days'
  AND si.stripe_subscription_id IS NULL
GROUP BY fs.status, fs.payment_provider
ORDER BY count DESC
""", "5. Unmatched FF Subs by Status and Payment Provider")

# 6. Check Stripe subscription status for unmatched FF subs
run_query("""
WITH stripe_subscription_charges AS (
    SELECT DISTINCT i.subscription AS stripe_subscription_id
    FROM raw_stripe.charges c
    JOIN raw_stripe.invoices i ON c.invoice = i.id
    WHERE c.status = 'succeeded'
      AND c.description = 'Subscription creation'
      AND c.created >= CURRENT_DATE - INTERVAL '30 days'
),
unmatched_ff AS (
    SELECT fs.psp_id
    FROM raw_funnelfox.subscriptions fs
    LEFT JOIN stripe_subscription_charges sc ON fs.psp_id = sc.stripe_subscription_id
    WHERE fs.sandbox = false
      AND fs.price NOT IN (100, 200)
      AND fs.created_at >= CURRENT_DATE - INTERVAL '30 days'
      AND sc.stripe_subscription_id IS NULL
)
SELECT
    ss.status AS stripe_status,
    COUNT(*) AS count
FROM unmatched_ff uf
JOIN raw_stripe.subscriptions ss ON uf.psp_id = ss.id
GROUP BY ss.status
ORDER BY count DESC
""", "6. Stripe Status of Unmatched FunnelFox Subscriptions")

# 7. Final: What revenue should mart_marketing_attribution actually show?
run_query("""
-- This is the CORRECT revenue: only count FunnelFox subs that have actual Stripe charges
WITH stripe_subscription_charges AS (
    SELECT
        c.id AS charge_id,
        c.amount / 100.0 AS stripe_charge_amount,
        i.subscription AS stripe_subscription_id
    FROM raw_stripe.charges c
    JOIN raw_stripe.invoices i ON c.invoice = i.id
    WHERE c.status = 'succeeded'
      AND c.description = 'Subscription creation'
      AND c.amount NOT IN (100, 200)
      AND c.created >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT
    COUNT(*) AS subscriptions,
    ROUND(SUM(sc.stripe_charge_amount)::numeric, 2) AS actual_stripe_revenue,
    ROUND(SUM(ff.price / 100.0)::numeric, 2) AS ff_configured_price
FROM stripe_subscription_charges sc
JOIN raw_funnelfox.subscriptions ff ON sc.stripe_subscription_id = ff.psp_id
WHERE ff.sandbox = false
""", "7. CORRECT Revenue (only charged subscriptions)")

conn.close()
print("\n\nDiagnosis complete!")
