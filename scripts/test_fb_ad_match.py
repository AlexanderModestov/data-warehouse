"""Test Facebook ad matching via Amplitude utm_campaign."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))
from lib.db import execute_query

print("Testing Facebook ad matching via Amplitude utm_campaign")
print("=" * 70)

query = """
WITH amplitude_with_session AS (
    SELECT
        SUBSTRING(event_properties->>'[Amplitude] Page Location' FROM 'fsid=([A-Z0-9]+)') AS ff_session_id,
        NULLIF(user_properties->>'initial_utm_campaign', 'EMPTY') AS utm_campaign,
        NULLIF(user_properties->>'initial_fbclid', 'EMPTY') AS fbclid
    FROM raw_amplitude.events
),
amplitude_attribution AS (
    SELECT DISTINCT ON (ff_session_id)
        ff_session_id AS session_id,
        utm_campaign,
        fbclid
    FROM amplitude_with_session
    WHERE ff_session_id IS NOT NULL
    ORDER BY ff_session_id
),
stripe_sessions AS (
    SELECT
        ss.metadata->>'ff_session_id' as session_id,
        SUM(c.amount / 100.0) as revenue_usd
    FROM raw_stripe.subscriptions ss
    LEFT JOIN raw_stripe.invoices i ON ss.id = i.subscription
    LEFT JOIN raw_stripe.charges c ON i.id = c.invoice AND c.status = 'succeeded'
    WHERE ss.metadata->>'ff_session_id' IS NOT NULL
    GROUP BY ss.metadata->>'ff_session_id'
),
facebook_ads AS (
    SELECT DISTINCT ON (facebook_ad_id)
        facebook_ad_id,
        facebook_adset_id,
        facebook_campaign_id,
        ad_name
    FROM raw_facebook.facebook_ads
    WHERE facebook_ad_id IS NOT NULL
    ORDER BY facebook_ad_id, created_time DESC
)
SELECT
    fb.facebook_ad_id IS NOT NULL as matched_to_fb_ad,
    COUNT(DISTINCT ss.session_id) as sessions,
    SUM(COALESCE(ss.revenue_usd, 0)) as revenue
FROM stripe_sessions ss
LEFT JOIN amplitude_attribution amp ON ss.session_id = amp.session_id
LEFT JOIN facebook_ads fb ON amp.utm_campaign = fb.facebook_ad_id
GROUP BY 1
ORDER BY revenue DESC
"""
result = execute_query(query)
print("Sessions with revenue - Facebook ad matching:")
print("-" * 70)
total_rev = 0
for r in result:
    rev = r['revenue'] if r['revenue'] else 0
    total_rev += rev
    print(f"  matched_to_fb_ad={r['matched_to_fb_ad']}: {r['sessions']} sessions, ${rev:,.2f}")
print(f"\nTotal revenue: ${total_rev:,.2f}")

# Show sample of matched sessions
print()
print("Sample matched sessions with Facebook ad info:")
print("-" * 70)
query2 = """
WITH amplitude_with_session AS (
    SELECT
        SUBSTRING(event_properties->>'[Amplitude] Page Location' FROM 'fsid=([A-Z0-9]+)') AS ff_session_id,
        NULLIF(user_properties->>'initial_utm_campaign', 'EMPTY') AS utm_campaign
    FROM raw_amplitude.events
),
amplitude_attribution AS (
    SELECT DISTINCT ON (ff_session_id)
        ff_session_id AS session_id,
        utm_campaign
    FROM amplitude_with_session
    WHERE ff_session_id IS NOT NULL
    ORDER BY ff_session_id
),
stripe_sessions AS (
    SELECT
        ss.metadata->>'ff_session_id' as session_id,
        SUM(c.amount / 100.0) as revenue_usd
    FROM raw_stripe.subscriptions ss
    LEFT JOIN raw_stripe.invoices i ON ss.id = i.subscription
    LEFT JOIN raw_stripe.charges c ON i.id = c.invoice AND c.status = 'succeeded'
    WHERE ss.metadata->>'ff_session_id' IS NOT NULL
    GROUP BY ss.metadata->>'ff_session_id'
),
facebook_ads AS (
    SELECT DISTINCT ON (facebook_ad_id)
        facebook_ad_id,
        facebook_campaign_id,
        ad_name
    FROM raw_facebook.facebook_ads
    WHERE facebook_ad_id IS NOT NULL
    ORDER BY facebook_ad_id, created_time DESC
)
SELECT
    ss.session_id,
    ss.revenue_usd,
    fb.facebook_ad_id,
    fb.facebook_campaign_id,
    SUBSTRING(fb.ad_name, 1, 50) as ad_name
FROM stripe_sessions ss
JOIN amplitude_attribution amp ON ss.session_id = amp.session_id
JOIN facebook_ads fb ON amp.utm_campaign = fb.facebook_ad_id
WHERE ss.revenue_usd > 0
LIMIT 10
"""
result2 = execute_query(query2)
for r in result2:
    print(f"  session={r['session_id'][:20]}...")
    print(f"    revenue=${r['revenue_usd']}, ad_id={r['facebook_ad_id']}")
    print(f"    campaign_id={r['facebook_campaign_id']}")
    print(f"    ad_name={r['ad_name']}")
    print()
