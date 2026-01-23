"""Investigate charges without invoice - how to link to FunnelFox sessions."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))
from lib.db import execute_query

print("Investigating charges without invoice linkage")
print("=" * 80)

# Check what metadata charges have
print("\n1. Sample charges WITHOUT invoice - check metadata")
print("-" * 80)
q1 = """
SELECT
    id,
    amount / 100.0 as amount_usd,
    metadata,
    payment_intent,
    customer
FROM raw_stripe.charges
WHERE status = 'succeeded'
  AND invoice IS NULL
LIMIT 5
"""
r1 = execute_query(q1)
for r in r1:
    print(f"   Charge: {r['id']}")
    print(f"   Amount: ${r['amount_usd']}")
    print(f"   Metadata: {r['metadata']}")
    print(f"   Payment Intent: {r['payment_intent']}")
    print(f"   Customer: {r['customer']}")
    print()

# Check payment_intent metadata
print("\n2. Check payment_intent metadata for session linkage")
print("-" * 80)
q2 = """
SELECT
    pi.id,
    pi.metadata,
    c.amount / 100.0 as charge_amount
FROM raw_stripe.payment_intents pi
JOIN raw_stripe.charges c ON pi.id = c.payment_intent
WHERE c.status = 'succeeded'
  AND c.invoice IS NULL
LIMIT 5
"""
try:
    r2 = execute_query(q2)
    for r in r2:
        print(f"   Payment Intent: {r['id']}")
        print(f"   Metadata: {r['metadata']}")
        print(f"   Charge Amount: ${r['charge_amount']}")
        print()
except Exception as e:
    print(f"   Error: {e}")

# Check if customer has linkage to FunnelFox
print("\n3. Check Stripe customer metadata for session linkage")
print("-" * 80)
q3 = """
SELECT
    cu.id,
    cu.metadata,
    cu.email,
    COUNT(c.id) as charge_count,
    SUM(c.amount / 100.0) as total_revenue
FROM raw_stripe.customers cu
JOIN raw_stripe.charges c ON cu.id = c.customer
WHERE c.status = 'succeeded'
  AND c.invoice IS NULL
GROUP BY cu.id, cu.metadata, cu.email
LIMIT 5
"""
try:
    r3 = execute_query(q3)
    for r in r3:
        print(f"   Customer: {r['id']}")
        print(f"   Email: {r['email']}")
        print(f"   Metadata: {r['metadata']}")
        print(f"   Charges: {r['charge_count']}, Revenue: ${r['total_revenue']}")
        print()
except Exception as e:
    print(f"   Error: {e}")

# Check if checkout_session has ff_session_id
print("\n4. Check checkout sessions for ff_session_id")
print("-" * 80)
q4 = """
SELECT COUNT(*) as total
FROM raw_stripe.checkout_sessions
WHERE metadata->>'ff_session_id' IS NOT NULL
"""
try:
    r4 = execute_query(q4)
    print(f"   Checkout sessions with ff_session_id: {r4[0]['total']}")
except Exception as e:
    print(f"   Checkout sessions table not available: {e}")

# Check checkout_session linkage to charges
print("\n5. Checkout sessions with payment_intent linkage")
print("-" * 80)
q5 = """
SELECT
    cs.id as checkout_session_id,
    cs.payment_intent,
    cs.metadata,
    c.amount / 100.0 as charge_amount
FROM raw_stripe.checkout_sessions cs
JOIN raw_stripe.payment_intents pi ON cs.payment_intent = pi.id
JOIN raw_stripe.charges c ON pi.id = c.payment_intent
WHERE c.status = 'succeeded'
  AND c.invoice IS NULL
LIMIT 5
"""
try:
    r5 = execute_query(q5)
    for r in r5:
        print(f"   Checkout Session: {r['checkout_session_id']}")
        print(f"   Metadata: {r['metadata']}")
        print(f"   Charge Amount: ${r['charge_amount']}")
        print()
except Exception as e:
    print(f"   Error: {e}")

# Check if there's a direct checkout_session to ff_session mapping
print("\n6. Total checkout sessions with ff_session_id and their revenue")
print("-" * 80)
q6 = """
SELECT
    COUNT(DISTINCT cs.id) as checkout_sessions,
    COUNT(c.id) as charges,
    SUM(c.amount / 100.0) as revenue_usd
FROM raw_stripe.checkout_sessions cs
JOIN raw_stripe.payment_intents pi ON cs.payment_intent = pi.id
JOIN raw_stripe.charges c ON pi.id = c.payment_intent
WHERE c.status = 'succeeded'
  AND cs.metadata->>'ff_session_id' IS NOT NULL
"""
try:
    r6 = execute_query(q6)
    rev = r6[0]['revenue_usd'] or 0
    print(f"   Checkout sessions with ff_session_id: {r6[0]['checkout_sessions']}")
    print(f"   Charges: {r6[0]['charges']}")
    print(f"   Revenue: ${rev:,.2f}")
except Exception as e:
    print(f"   Error: {e}")

# Alternative: Link via customer email -> FunnelFox profile
print("\n7. Alternative: Link via customer to FunnelFox profile")
print("-" * 80)
q7 = """
SELECT
    COUNT(DISTINCT c.customer) as unique_customers,
    COUNT(c.id) as total_charges,
    SUM(c.amount / 100.0) as revenue_usd
FROM raw_stripe.charges c
WHERE c.status = 'succeeded'
  AND c.invoice IS NULL
"""
r7 = execute_query(q7)
rev7 = r7[0]['revenue_usd'] or 0
print(f"   Customers with non-invoice charges: {r7[0]['unique_customers']}")
print(f"   Total charges: {r7[0]['total_charges']}")
print(f"   Total revenue: ${rev7:,.2f}")

# Check FunnelFox subscriptions linkage
print("\n8. FunnelFox subscriptions with psp_id matching Stripe")
print("-" * 80)
q8 = """
SELECT
    COUNT(*) as ff_subscriptions,
    COUNT(CASE WHEN psp_id IS NOT NULL THEN 1 END) as with_psp_id,
    SUM(price / 100.0) as total_price_usd
FROM raw_funnelfox.subscriptions
WHERE sandbox = false
"""
r8 = execute_query(q8)
price = r8[0]['total_price_usd'] or 0
print(f"   FunnelFox subscriptions (non-sandbox): {r8[0]['ff_subscriptions']}")
print(f"   With psp_id: {r8[0]['with_psp_id']}")
print(f"   Total subscription price: ${price:,.2f}")

# Check what psp_id looks like - is it charge ID, subscription ID, or payment_intent?
print("\n9. Sample FunnelFox psp_id values")
print("-" * 80)
q9 = """
SELECT psp_id, payment_provider, price / 100.0 as price_usd
FROM raw_funnelfox.subscriptions
WHERE psp_id IS NOT NULL AND sandbox = false
LIMIT 5
"""
r9 = execute_query(q9)
for r in r9:
    print(f"   psp_id: {r['psp_id']}")
    print(f"   Provider: {r['payment_provider']}, Price: ${r['price_usd']}")
    print()

# Check if psp_id matches Stripe charge ID
print("\n10. FunnelFox psp_id matching Stripe charges")
print("-" * 80)
q10 = """
SELECT
    COUNT(*) as matched_charges,
    SUM(c.amount / 100.0) as revenue_usd
FROM raw_funnelfox.subscriptions fs
JOIN raw_stripe.charges c ON fs.psp_id = c.id
WHERE fs.sandbox = false
  AND c.status = 'succeeded'
"""
r10 = execute_query(q10)
rev10 = r10[0]['revenue_usd'] or 0
print(f"   Matched charges (psp_id = charge.id): {r10[0]['matched_charges']}")
print(f"   Revenue: ${rev10:,.2f}")

# Check if psp_id matches Stripe payment_intent
print("\n11. FunnelFox psp_id matching Stripe payment_intents")
print("-" * 80)
q11 = """
SELECT
    COUNT(*) as matched_intents,
    SUM(c.amount / 100.0) as revenue_usd
FROM raw_funnelfox.subscriptions fs
JOIN raw_stripe.payment_intents pi ON fs.psp_id = pi.id
JOIN raw_stripe.charges c ON pi.id = c.payment_intent
WHERE fs.sandbox = false
  AND c.status = 'succeeded'
"""
try:
    r11 = execute_query(q11)
    rev11 = r11[0]['revenue_usd'] or 0
    print(f"   Matched via payment_intent: {r11[0]['matched_intents']}")
    print(f"   Revenue: ${rev11:,.2f}")
except Exception as e:
    print(f"   Error: {e}")

# Check if psp_id matches Stripe subscription
print("\n12. FunnelFox psp_id matching Stripe subscriptions")
print("-" * 80)
q12 = """
SELECT
    COUNT(*) as matched_subs
FROM raw_funnelfox.subscriptions fs
JOIN raw_stripe.subscriptions ss ON fs.psp_id = ss.id
WHERE fs.sandbox = false
"""
r12 = execute_query(q12)
print(f"   Matched via subscription ID: {r12[0]['matched_subs']}")
