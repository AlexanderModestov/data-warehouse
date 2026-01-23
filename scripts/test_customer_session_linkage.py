"""Test revenue linkage via Stripe customer metadata ff_session_id."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))
from lib.db import execute_query

print("Testing Revenue Linkage via Customer Metadata")
print("=" * 80)

# Revenue via customer -> ff_session_id -> FunnelFox session
print("\n1. Charges linked via customer.metadata.ff_session_id")
print("-" * 80)
q1 = """
SELECT
    COUNT(c.id) as charges,
    SUM(c.amount / 100.0) as revenue_usd
FROM raw_stripe.charges c
JOIN raw_stripe.customers cu ON c.customer = cu.id
WHERE c.status = 'succeeded'
  AND cu.metadata->>'ff_session_id' IS NOT NULL
"""
r1 = execute_query(q1)
rev1 = r1[0]['revenue_usd'] or 0
print(f"   Charges with customer ff_session_id: {r1[0]['charges']}")
print(f"   Revenue: ${rev1:,.2f}")

# How many match FunnelFox sessions?
print("\n2. Customer ff_session_id matching FunnelFox sessions")
print("-" * 80)
q2 = """
SELECT
    COUNT(c.id) as charges,
    SUM(c.amount / 100.0) as revenue_usd,
    COUNT(DISTINCT cu.metadata->>'ff_session_id') as unique_sessions
FROM raw_stripe.charges c
JOIN raw_stripe.customers cu ON c.customer = cu.id
JOIN raw_funnelfox.sessions fs ON cu.metadata->>'ff_session_id' = fs.id
WHERE c.status = 'succeeded'
  AND cu.metadata->>'ff_session_id' IS NOT NULL
"""
r2 = execute_query(q2)
rev2 = r2[0]['revenue_usd'] or 0
print(f"   Matched charges: {r2[0]['charges']}")
print(f"   Matched revenue: ${rev2:,.2f}")
print(f"   Unique sessions: {r2[0]['unique_sessions']}")

# Compare with previous approach (subscription metadata)
print("\n3. Previous approach: subscription.metadata.ff_session_id -> invoice -> charge")
print("-" * 80)
q3 = """
SELECT
    COUNT(c.id) as charges,
    SUM(c.amount / 100.0) as revenue_usd
FROM raw_stripe.subscriptions ss
JOIN raw_stripe.invoices i ON ss.id = i.subscription
JOIN raw_stripe.charges c ON i.id = c.invoice
WHERE ss.metadata->>'ff_session_id' IS NOT NULL
  AND c.status = 'succeeded'
"""
r3 = execute_query(q3)
rev3 = r3[0]['revenue_usd'] or 0
print(f"   Charges: {r3[0]['charges']}")
print(f"   Revenue: ${rev3:,.2f}")

# Total comparison
print("\n" + "=" * 80)
print("SUMMARY: Revenue Coverage Comparison")
print("=" * 80)
total_rev = """
SELECT SUM(amount / 100.0) as revenue_usd
FROM raw_stripe.charges
WHERE status = 'succeeded'
"""
r_total = execute_query(total_rev)
total = r_total[0]['revenue_usd'] or 0

print(f"   Via customer.metadata.ff_session_id:     ${rev2:,.2f} ({rev2/total*100:.1f}%)")
print(f"   Via subscription -> invoice -> charge:   ${rev3:,.2f} ({rev3/total*100:.1f}%)")
print(f"   Total Stripe revenue:                    ${total:,.2f}")

# Check for overlap
print("\n4. Check for overlap between two methods")
print("-" * 80)
q4 = """
WITH customer_charges AS (
    SELECT c.id as charge_id
    FROM raw_stripe.charges c
    JOIN raw_stripe.customers cu ON c.customer = cu.id
    JOIN raw_funnelfox.sessions fs ON cu.metadata->>'ff_session_id' = fs.id
    WHERE c.status = 'succeeded'
),
subscription_charges AS (
    SELECT c.id as charge_id
    FROM raw_stripe.subscriptions ss
    JOIN raw_stripe.invoices i ON ss.id = i.subscription
    JOIN raw_stripe.charges c ON i.id = c.invoice
    WHERE ss.metadata->>'ff_session_id' IS NOT NULL
      AND c.status = 'succeeded'
)
SELECT
    (SELECT COUNT(*) FROM customer_charges) as customer_method,
    (SELECT COUNT(*) FROM subscription_charges) as subscription_method,
    (SELECT COUNT(*) FROM customer_charges cc JOIN subscription_charges sc ON cc.charge_id = sc.charge_id) as overlap
"""
r4 = execute_query(q4)
print(f"   Customer method charges: {r4[0]['customer_method']}")
print(f"   Subscription method charges: {r4[0]['subscription_method']}")
print(f"   Overlap: {r4[0]['overlap']}")

# Unique charges per method
print("\n5. Unique charges per method (union)")
print("-" * 80)
q5 = """
WITH customer_charges AS (
    SELECT c.id as charge_id, c.amount / 100.0 as amount_usd
    FROM raw_stripe.charges c
    JOIN raw_stripe.customers cu ON c.customer = cu.id
    JOIN raw_funnelfox.sessions fs ON cu.metadata->>'ff_session_id' = fs.id
    WHERE c.status = 'succeeded'
),
subscription_charges AS (
    SELECT c.id as charge_id, c.amount / 100.0 as amount_usd
    FROM raw_stripe.subscriptions ss
    JOIN raw_stripe.invoices i ON ss.id = i.subscription
    JOIN raw_stripe.charges c ON i.id = c.invoice
    JOIN raw_funnelfox.sessions fs ON ss.metadata->>'ff_session_id' = fs.id
    WHERE c.status = 'succeeded'
),
all_charges AS (
    SELECT charge_id, amount_usd FROM customer_charges
    UNION
    SELECT charge_id, amount_usd FROM subscription_charges
)
SELECT
    COUNT(*) as total_unique_charges,
    SUM(amount_usd) as total_revenue
FROM all_charges
"""
r5 = execute_query(q5)
rev5 = r5[0]['total_revenue'] or 0
print(f"   Total unique charges (union): {r5[0]['total_unique_charges']}")
print(f"   Total revenue (union): ${rev5:,.2f} ({rev5/total*100:.1f}%)")

print("\n" + "=" * 80)
print("RECOMMENDATION: Use customer.metadata.ff_session_id as PRIMARY linkage")
print("=" * 80)
