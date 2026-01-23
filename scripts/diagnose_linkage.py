"""
Diagnose the linkage between Stripe charges, invoices, and subscriptions.
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

# 1. Check if invoices table exists and has subscription data
run_query("""
SELECT COUNT(*) AS invoice_count,
       COUNT(subscription) AS with_subscription,
       COUNT(*) - COUNT(subscription) AS without_subscription
FROM raw_stripe.invoices
""", "1. Invoices Table Overview")

# 2. Sample of subscription creation charges
run_query("""
SELECT
    id AS charge_id,
    invoice,
    customer,
    amount / 100.0 AS amount_usd,
    LEFT(description, 40) AS description,
    created::date AS created_date
FROM raw_stripe.charges
WHERE status = 'succeeded'
  AND description = 'Subscription creation'
  AND amount NOT IN (100, 200)
ORDER BY created DESC
LIMIT 5
""", "2. Sample Subscription Creation Charges")

# 3. Check if charges have invoice IDs
run_query("""
SELECT
    CASE WHEN invoice IS NOT NULL THEN 'Has invoice' ELSE 'No invoice' END AS invoice_status,
    COUNT(*) AS count,
    ROUND(SUM(amount / 100.0)::numeric, 2) AS total_usd
FROM raw_stripe.charges
WHERE status = 'succeeded'
  AND description = 'Subscription creation'
  AND amount NOT IN (100, 200)
  AND created >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY CASE WHEN invoice IS NOT NULL THEN 'Has invoice' ELSE 'No invoice' END
""", "3. Charges With/Without Invoice (Last 30 Days)")

# 4. Check invoice -> subscription linkage
run_query("""
SELECT
    CASE WHEN i.subscription IS NOT NULL THEN 'Has subscription' ELSE 'No subscription' END AS sub_status,
    COUNT(*) AS count
FROM raw_stripe.charges c
JOIN raw_stripe.invoices i ON c.invoice = i.id
WHERE c.status = 'succeeded'
  AND c.description = 'Subscription creation'
  AND c.amount NOT IN (100, 200)
  AND c.created >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY CASE WHEN i.subscription IS NOT NULL THEN 'Has subscription' ELSE 'No subscription' END
""", "4. Invoice -> Subscription Linkage")

# 5. Direct check: can we link charge -> invoice -> subscription -> funnelfox?
run_query("""
SELECT
    c.id AS charge_id,
    c.invoice,
    i.subscription AS stripe_sub_id,
    ff.id AS ff_sub_id,
    c.amount / 100.0 AS charge_amount,
    ff.price / 100.0 AS ff_price
FROM raw_stripe.charges c
LEFT JOIN raw_stripe.invoices i ON c.invoice = i.id
LEFT JOIN raw_funnelfox.subscriptions ff ON i.subscription = ff.psp_id AND ff.sandbox = false
WHERE c.status = 'succeeded'
  AND c.description = 'Subscription creation'
  AND c.amount NOT IN (100, 200)
ORDER BY c.created DESC
LIMIT 10
""", "5. Full Linkage Chain (charge -> invoice -> subscription -> FF)")

# 6. Check FunnelFox psp_id values
run_query("""
SELECT
    LEFT(psp_id, 10) AS psp_id_prefix,
    COUNT(*) AS count
FROM raw_funnelfox.subscriptions
WHERE sandbox = false
  AND created_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY LEFT(psp_id, 10)
ORDER BY count DESC
""", "6. FunnelFox psp_id Prefixes")

# 7. Sample FunnelFox subscription psp_ids vs Stripe subscription IDs
run_query("""
SELECT
    ff.psp_id AS ff_psp_id,
    ss.id AS stripe_sub_id,
    CASE WHEN ff.psp_id = ss.id THEN 'MATCH' ELSE 'NO MATCH' END AS match_status
FROM raw_funnelfox.subscriptions ff
CROSS JOIN LATERAL (
    SELECT id FROM raw_stripe.subscriptions
    WHERE id = ff.psp_id
    LIMIT 1
) ss
WHERE ff.sandbox = false
  AND ff.created_at >= CURRENT_DATE - INTERVAL '30 days'
LIMIT 5
""", "7. Sample psp_id Match Check")

# 8. Alternative: Match via Stripe customer + timestamp
run_query("""
WITH stripe_charges AS (
    SELECT
        id AS charge_id,
        customer,
        amount / 100.0 AS amount_usd,
        created
    FROM raw_stripe.charges
    WHERE status = 'succeeded'
      AND description = 'Subscription creation'
      AND amount NOT IN (100, 200)
      AND created >= CURRENT_DATE - INTERVAL '30 days'
),
stripe_subs AS (
    SELECT
        id AS stripe_sub_id,
        customer,
        created AS sub_created
    FROM raw_stripe.subscriptions
    WHERE created >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT
    COUNT(DISTINCT sc.charge_id) AS charges,
    COUNT(DISTINCT ss.stripe_sub_id) AS matched_stripe_subs,
    COUNT(DISTINCT ff.id) AS matched_ff_subs,
    ROUND(SUM(DISTINCT sc.amount_usd)::numeric, 2) AS stripe_revenue,
    ROUND(SUM(DISTINCT ff.price / 100.0)::numeric, 2) AS ff_price
FROM stripe_charges sc
LEFT JOIN stripe_subs ss
    ON sc.customer = ss.customer
    AND ss.sub_created >= sc.created - INTERVAL '1 minute'
    AND ss.sub_created <= sc.created + INTERVAL '1 minute'
LEFT JOIN raw_funnelfox.subscriptions ff
    ON ss.stripe_sub_id = ff.psp_id
    AND ff.sandbox = false
""", "8. Alternative Matching via Customer + Timestamp")

conn.close()
print("\n\nDiagnosis complete!")
