"""Diagnose revenue matching between FunnelFox sessions and Stripe charges."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))
from lib.db import execute_query

print("Revenue Matching Diagnostic")
print("=" * 80)

# Step 1: Total FunnelFox sessions
print("\n1. FunnelFox Sessions (base)")
print("-" * 80)
q1 = """
SELECT
    COUNT(*) as total_sessions,
    COUNT(DISTINCT profile_id) as unique_profiles
FROM raw_funnelfox.sessions
"""
r1 = execute_query(q1)
print(f"   Total sessions: {r1[0]['total_sessions']:,}")
print(f"   Unique profiles: {r1[0]['unique_profiles']:,}")

# Step 2: Stripe subscriptions with ff_session_id
print("\n2. Stripe Subscriptions with ff_session_id in metadata")
print("-" * 80)
q2 = """
SELECT
    COUNT(*) as subscriptions_with_session,
    COUNT(DISTINCT metadata->>'ff_session_id') as unique_sessions
FROM raw_stripe.subscriptions
WHERE metadata->>'ff_session_id' IS NOT NULL
"""
r2 = execute_query(q2)
print(f"   Subscriptions with ff_session_id: {r2[0]['subscriptions_with_session']:,}")
print(f"   Unique session IDs: {r2[0]['unique_sessions']:,}")

# Step 3: How many Stripe subscriptions match FunnelFox sessions
print("\n3. Stripe Subscriptions that match FunnelFox sessions")
print("-" * 80)
q3 = """
SELECT
    COUNT(*) as matched_subscriptions,
    COUNT(DISTINCT ss.metadata->>'ff_session_id') as matched_sessions
FROM raw_stripe.subscriptions ss
JOIN raw_funnelfox.sessions fs ON ss.metadata->>'ff_session_id' = fs.id
WHERE ss.metadata->>'ff_session_id' IS NOT NULL
"""
r3 = execute_query(q3)
print(f"   Matched subscriptions: {r3[0]['matched_subscriptions']:,}")
print(f"   Matched sessions: {r3[0]['matched_sessions']:,}")

# Step 4: Invoices linked to those subscriptions
print("\n4. Invoices linked to subscriptions with ff_session_id")
print("-" * 80)
q4 = """
SELECT
    COUNT(DISTINCT ss.id) as subscriptions_with_invoices,
    COUNT(i.id) as total_invoices
FROM raw_stripe.subscriptions ss
JOIN raw_stripe.invoices i ON ss.id = i.subscription
WHERE ss.metadata->>'ff_session_id' IS NOT NULL
"""
r4 = execute_query(q4)
print(f"   Subscriptions with invoices: {r4[0]['subscriptions_with_invoices']:,}")
print(f"   Total invoices: {r4[0]['total_invoices']:,}")

# Step 5: Successful charges linked via invoices
print("\n5. Successful charges via subscription -> invoice -> charge path")
print("-" * 80)
q5 = """
SELECT
    COUNT(DISTINCT ss.id) as subscriptions_with_charges,
    COUNT(c.id) as total_charges,
    SUM(c.amount / 100.0) as total_revenue_usd
FROM raw_stripe.subscriptions ss
JOIN raw_stripe.invoices i ON ss.id = i.subscription
JOIN raw_stripe.charges c ON i.id = c.invoice
WHERE ss.metadata->>'ff_session_id' IS NOT NULL
  AND c.status = 'succeeded'
"""
r5 = execute_query(q5)
print(f"   Subscriptions with successful charges: {r5[0]['subscriptions_with_charges']:,}")
print(f"   Total successful charges: {r5[0]['total_charges']:,}")
rev = r5[0]['total_revenue_usd'] or 0
print(f"   Total revenue: ${rev:,.2f}")

# Step 6: Check for charges WITHOUT invoice linkage (one-time purchases?)
print("\n6. Alternative: Charges with ff_session_id directly in metadata")
print("-" * 80)
q6 = """
SELECT
    COUNT(*) as charges_with_session,
    SUM(amount / 100.0) as revenue_usd
FROM raw_stripe.charges
WHERE status = 'succeeded'
  AND metadata->>'ff_session_id' IS NOT NULL
"""
r6 = execute_query(q6)
charges = r6[0]['charges_with_session'] or 0
rev6 = r6[0]['revenue_usd'] or 0
print(f"   Charges with ff_session_id in charge metadata: {charges:,}")
print(f"   Revenue: ${rev6:,.2f}")

# Step 7: Check payment_intent metadata
print("\n7. Alternative: Payment intents with ff_session_id")
print("-" * 80)
q7 = """
SELECT
    COUNT(*) as payment_intents_with_session
FROM raw_stripe.payment_intents
WHERE metadata->>'ff_session_id' IS NOT NULL
"""
try:
    r7 = execute_query(q7)
    print(f"   Payment intents with ff_session_id: {r7[0]['payment_intents_with_session']:,}")
except Exception as e:
    print(f"   Payment intents table not available or no metadata: {e}")

# Step 8: Check total Stripe revenue for comparison
print("\n8. Total Stripe revenue (for comparison)")
print("-" * 80)
q8 = """
SELECT
    COUNT(*) as total_charges,
    SUM(amount / 100.0) as total_revenue_usd
FROM raw_stripe.charges
WHERE status = 'succeeded'
"""
r8 = execute_query(q8)
print(f"   Total successful charges: {r8[0]['total_charges']:,}")
rev8 = r8[0]['total_revenue_usd'] or 0
print(f"   Total Stripe revenue: ${rev8:,.2f}")

# Step 9: What percentage of revenue can we attribute?
print("\n" + "=" * 80)
print("SUMMARY: Revenue Attribution Coverage")
print("=" * 80)
if rev8 > 0:
    pct = (rev / rev8) * 100 if rev else 0
    print(f"   Revenue via subscription->invoice->charge: ${rev:,.2f} ({pct:.1f}%)")
    print(f"   Total Stripe revenue:                    ${rev8:,.2f} (100%)")
    print(f"   Gap (unattributed):                      ${rev8-rev:,.2f} ({100-pct:.1f}%)")

# Step 10: Breakdown of charges by linkage type
print("\n9. Charge breakdown by linkage availability")
print("-" * 80)
q10 = """
WITH charge_linkage AS (
    SELECT
        c.id,
        c.amount / 100.0 as amount_usd,
        CASE
            WHEN c.invoice IS NOT NULL THEN 'has_invoice'
            ELSE 'no_invoice'
        END as invoice_status,
        CASE
            WHEN i.subscription IS NOT NULL THEN 'has_subscription'
            ELSE 'no_subscription'
        END as subscription_status
    FROM raw_stripe.charges c
    LEFT JOIN raw_stripe.invoices i ON c.invoice = i.id
    WHERE c.status = 'succeeded'
)
SELECT
    invoice_status,
    subscription_status,
    COUNT(*) as charges,
    SUM(amount_usd) as revenue_usd
FROM charge_linkage
GROUP BY invoice_status, subscription_status
ORDER BY revenue_usd DESC
"""
r10 = execute_query(q10)
for r in r10:
    rev_r = r['revenue_usd'] or 0
    print(f"   {r['invoice_status']} / {r['subscription_status']}: {r['charges']:,} charges, ${rev_r:,.2f}")

# Step 11: Check if subscriptions without invoices have ff_session_id
print("\n10. Subscriptions WITH ff_session_id but WITHOUT invoices")
print("-" * 80)
q11 = """
SELECT
    COUNT(*) as subs_without_invoices
FROM raw_stripe.subscriptions ss
LEFT JOIN raw_stripe.invoices i ON ss.id = i.subscription
WHERE ss.metadata->>'ff_session_id' IS NOT NULL
  AND i.id IS NULL
"""
r11 = execute_query(q11)
print(f"   Subscriptions with ff_session_id but no invoices: {r11[0]['subs_without_invoices']:,}")
