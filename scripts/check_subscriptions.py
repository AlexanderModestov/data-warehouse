"""Check subscription data samples to understand linkage."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))
from lib.db import execute_query

# Check what's in FunnelFox subscriptions
print("FunnelFox subscriptions sample:")
print("-" * 60)
result = execute_query("""
    SELECT
        profile_id,
        psp_id,
        price / 100.0 as price_usd
    FROM raw_funnelfox.subscriptions
    WHERE sandbox = FALSE
    LIMIT 5
""")
for r in result:
    print(f"  profile_id={r['profile_id']}, psp_id={r['psp_id']}, price=${r['price_usd']}")

# Check Stripe subscriptions for ff_session_id
print()
print("Stripe subscriptions sample (with metadata):")
print("-" * 60)
result2 = execute_query("""
    SELECT
        id,
        customer,
        metadata::text as meta,
        status
    FROM raw_stripe.subscriptions
    LIMIT 5
""")
for r in result2:
    meta = r['meta'][:100] if r['meta'] else 'NULL'
    print(f"  id={r['id']}")
    print(f"    customer={r['customer']}")
    print(f"    metadata={meta}")
    print()

# Check if we can link via psp_id (sub_xxx) to Stripe subscriptions
print()
print("Link FF subscriptions -> Stripe subscriptions -> Charges:")
print("-" * 60)
result3 = execute_query("""
    SELECT
        COUNT(*) as ff_subs,
        COUNT(ss.id) as matched_stripe_subs,
        COUNT(c.id) as matched_charges,
        SUM(CASE WHEN c.id IS NOT NULL THEN c.amount / 100.0 ELSE 0 END) as revenue
    FROM raw_funnelfox.subscriptions fs
    LEFT JOIN raw_stripe.subscriptions ss ON fs.psp_id = ss.id
    LEFT JOIN raw_stripe.invoices i ON ss.id = i.subscription
    LEFT JOIN raw_stripe.charges c ON i.id = c.invoice AND c.status = 'succeeded'
    WHERE fs.sandbox = FALSE
""")
for r in result3:
    print(f"  FF subscriptions:        {r['ff_subs']}")
    print(f"  Matched Stripe subs:     {r['matched_stripe_subs']}")
    print(f"  Matched charges:         {r['matched_charges']}")
    rev = r['revenue'] if r['revenue'] else 0
    print(f"  Revenue from charges:    ${rev:,.2f}")
