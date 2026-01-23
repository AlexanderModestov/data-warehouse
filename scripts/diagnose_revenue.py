"""
Diagnostic script to check revenue linkage between sessions, subscriptions, and charges.
"""
import sys
import os

# Add dashboard lib to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))

from lib.db import execute_query


def main():
    print("=" * 60)
    print("REVENUE LINKAGE DIAGNOSTIC")
    print("=" * 60)

    # Query 1: Check overlap between sessions and subscriptions via profile_id
    print("\n1. Profile ID linkage (sessions <-> subscriptions):")
    print("-" * 60)
    query1 = """
        SELECT
            COUNT(DISTINCT s.profile_id) as session_profiles,
            COUNT(DISTINCT sub.profile_id) as subscription_profiles,
            COUNT(DISTINCT CASE WHEN sub.profile_id IS NOT NULL THEN s.profile_id END) as matched_profiles
        FROM raw_funnelfox.sessions s
        LEFT JOIN raw_funnelfox.subscriptions sub
            ON s.profile_id = sub.profile_id
            AND sub.sandbox = FALSE
    """
    result = execute_query(query1)
    for row in result:
        print(f"  Session profiles:      {row['session_profiles']:,}")
        print(f"  Subscription profiles: {row['subscription_profiles']:,}")
        print(f"  Matched profiles:      {row['matched_profiles']:,}")

    # Query 2: Check what psp_id looks like
    print("\n2. FunnelFox psp_id prefixes (linkage type):")
    print("-" * 60)
    query2 = """
        SELECT
            LEFT(psp_id, 3) as prefix,
            COUNT(*) as cnt
        FROM raw_funnelfox.subscriptions
        WHERE sandbox = FALSE
          AND psp_id IS NOT NULL
        GROUP BY LEFT(psp_id, 3)
        ORDER BY cnt DESC
        LIMIT 10
    """
    result = execute_query(query2)
    for row in result:
        prefix = row['prefix']
        link_type = {
            'ch_': 'charge_id',
            'pi_': 'payment_intent',
            'sub': 'subscription_id',
            'cus': 'customer_id'
        }.get(prefix, 'unknown')
        print(f"  {prefix}: {row['cnt']:,} ({link_type})")

    # Query 3: Check if we can link via psp_id to Stripe charges
    print("\n3. psp_id -> Stripe charges linkage:")
    print("-" * 60)
    query3 = """
        SELECT
            COUNT(*) as total_ff_subs,
            COUNT(c.id) as matched_to_charges,
            SUM(CASE WHEN c.id IS NOT NULL THEN c.amount / 100.0 ELSE 0 END) as stripe_revenue,
            SUM(sub.price / 100.0) as ff_revenue
        FROM raw_funnelfox.subscriptions sub
        LEFT JOIN raw_stripe.charges c ON sub.psp_id = c.id
        WHERE sub.sandbox = FALSE
    """
    result = execute_query(query3)
    for row in result:
        print(f"  Total FF subscriptions:  {row['total_ff_subs']:,}")
        print(f"  Matched to charges:      {row['matched_to_charges']:,}")
        print(f"  Stripe revenue (USD):    ${row['stripe_revenue']:,.2f}")
        print(f"  FunnelFox revenue (USD): ${row['ff_revenue']:,.2f}")

    # Query 4: Check payment_intent linkage
    print("\n4. psp_id -> payment_intent -> charges linkage:")
    print("-" * 60)
    query4 = """
        SELECT
            COUNT(*) as total_ff_subs,
            COUNT(c.id) as matched_via_intent,
            SUM(CASE WHEN c.id IS NOT NULL THEN c.amount / 100.0 ELSE 0 END) as stripe_revenue
        FROM raw_funnelfox.subscriptions sub
        LEFT JOIN raw_stripe.charges c ON sub.psp_id = c.payment_intent
        WHERE sub.sandbox = FALSE
          AND sub.psp_id LIKE 'pi_%'
    """
    result = execute_query(query4)
    for row in result:
        print(f"  FF subs with pi_ prefix: {row['total_ff_subs']:,}")
        print(f"  Matched via intent:      {row['matched_via_intent']:,}")
        print(f"  Stripe revenue (USD):    ${row['stripe_revenue']:,.2f}")

    # Query 5: Sessions with conversions that have revenue
    print("\n5. Sessions -> Subscriptions -> Revenue flow:")
    print("-" * 60)
    query5 = """
        SELECT
            COUNT(DISTINCT s.id) as total_sessions,
            COUNT(DISTINCT CASE WHEN sub.id IS NOT NULL THEN s.id END) as sessions_with_sub,
            COUNT(DISTINCT sub.id) as unique_subscriptions,
            SUM(sub.price / 100.0) as total_ff_revenue
        FROM raw_funnelfox.sessions s
        LEFT JOIN raw_funnelfox.subscriptions sub
            ON s.profile_id = sub.profile_id
            AND sub.sandbox = FALSE
    """
    result = execute_query(query5)
    for row in result:
        print(f"  Total sessions:          {row['total_sessions']:,}")
        print(f"  Sessions with sub:       {row['sessions_with_sub']:,}")
        print(f"  Unique subscriptions:    {row['unique_subscriptions']:,}")
        print(f"  Total FF revenue (USD):  ${row['total_ff_revenue']:,.2f}" if row['total_ff_revenue'] else "  Total FF revenue (USD):  $0.00")

    print("\n" + "=" * 60)
    print("DIAGNOSIS COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
