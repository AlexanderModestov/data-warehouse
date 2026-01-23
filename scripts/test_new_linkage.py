"""Test the new session → Stripe subscription → charges linkage."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))
from lib.db import execute_query

print("Testing new linkage: Session -> Stripe Subscription (metadata) -> Charges")
print("=" * 70)

# Test the new linkage
query = """
WITH stripe_subscriptions_with_session AS (
    SELECT
        id AS stripe_subscription_id,
        customer AS stripe_customer_id,
        metadata->>'ff_session_id' AS ff_session_id,
        status AS subscription_status,
        created AS subscription_created_at
    FROM raw_stripe.subscriptions
    WHERE metadata->>'ff_session_id' IS NOT NULL
),
stripe_subscription_charges AS (
    SELECT
        ss.ff_session_id,
        ss.stripe_subscription_id,
        c.id AS charge_id,
        c.amount / 100.0 AS charge_amount_usd
    FROM stripe_subscriptions_with_session ss
    LEFT JOIN raw_stripe.invoices i
        ON ss.stripe_subscription_id = i.subscription
    LEFT JOIN raw_stripe.charges c
        ON i.id = c.invoice
        AND c.status = 'succeeded'
),
session_conversions AS (
    SELECT
        ff_session_id AS session_id,
        SUM(COALESCE(charge_amount_usd, 0)) AS revenue_usd,
        COUNT(charge_id) AS charge_count
    FROM stripe_subscription_charges
    GROUP BY ff_session_id
)
SELECT
    COUNT(*) AS sessions_with_conversions,
    SUM(revenue_usd) AS total_revenue,
    SUM(charge_count) AS total_charges
FROM session_conversions
WHERE revenue_usd > 0
"""

result = execute_query(query)
for r in result:
    print(f"Sessions with conversions: {r['sessions_with_conversions']}")
    print(f"Total revenue (USD):       ${r['total_revenue']:,.2f}" if r['total_revenue'] else "Total revenue: $0.00")
    print(f"Total charges:             {r['total_charges']}")

# Now test joining to FunnelFox sessions
print()
print("Testing join to FunnelFox sessions:")
print("-" * 70)

query2 = """
WITH stripe_subscriptions_with_session AS (
    SELECT
        id AS stripe_subscription_id,
        metadata->>'ff_session_id' AS ff_session_id,
        created AS subscription_created_at
    FROM raw_stripe.subscriptions
    WHERE metadata->>'ff_session_id' IS NOT NULL
),
stripe_subscription_charges AS (
    SELECT
        ss.ff_session_id,
        c.amount / 100.0 AS charge_amount_usd
    FROM stripe_subscriptions_with_session ss
    LEFT JOIN raw_stripe.invoices i ON ss.stripe_subscription_id = i.subscription
    LEFT JOIN raw_stripe.charges c ON i.id = c.invoice AND c.status = 'succeeded'
),
session_conversions AS (
    SELECT
        ff_session_id AS session_id,
        SUM(COALESCE(charge_amount_usd, 0)) AS revenue_usd
    FROM stripe_subscription_charges
    GROUP BY ff_session_id
)
SELECT
    COUNT(DISTINCT s.id) AS total_ff_sessions,
    COUNT(DISTINCT CASE WHEN conv.session_id IS NOT NULL THEN s.id END) AS matched_sessions,
    SUM(COALESCE(conv.revenue_usd, 0)) AS total_revenue
FROM raw_funnelfox.sessions s
LEFT JOIN session_conversions conv ON s.id = conv.session_id
"""

result2 = execute_query(query2)
for r in result2:
    print(f"Total FunnelFox sessions:  {r['total_ff_sessions']:,}")
    print(f"Matched to conversions:    {r['matched_sessions']:,}")
    rev = r['total_revenue'] if r['total_revenue'] else 0
    print(f"Total revenue (USD):       ${rev:,.2f}")

print()
print("=" * 70)
print("If revenue > 0, the new linkage is working!")
