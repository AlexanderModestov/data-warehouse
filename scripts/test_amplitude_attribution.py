"""Test the new Amplitude attribution linkage."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))
from lib.db import execute_query

print("Testing Amplitude attribution -> Facebook campaign linkage")
print("=" * 70)

# Simulate the new model logic
query = """
WITH amplitude_with_session AS (
    SELECT
        *,
        SUBSTRING(event_properties->>'[Amplitude] Page Location' FROM 'fsid=([A-Z0-9]+)') AS ff_session_id
    FROM raw_amplitude.events
),
amplitude_attribution AS (
    SELECT DISTINCT ON (ff_session_id)
        ff_session_id AS session_id,
        NULLIF(user_properties->>'initial_utm_source', 'EMPTY') AS amp_utm_source,
        NULLIF(user_properties->>'initial_utm_medium', 'EMPTY') AS amp_utm_medium,
        NULLIF(user_properties->>'initial_utm_campaign', 'EMPTY') AS amp_utm_campaign,
        NULLIF(user_properties->>'initial_fbclid', 'EMPTY') AS amp_fbclid
    FROM amplitude_with_session
    WHERE ff_session_id IS NOT NULL
      AND (user_properties->>'initial_utm_source' IS NOT NULL
           OR user_properties->>'initial_fbclid' IS NOT NULL)
    ORDER BY ff_session_id, event_time ASC
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
facebook_campaigns AS (
    SELECT DISTINCT ON (campaign_name)
        facebook_campaign_id,
        campaign_name
    FROM raw_facebook.facebook_campaigns
    WHERE campaign_name IS NOT NULL
    ORDER BY campaign_name, created_time DESC
)
SELECT
    CASE
        WHEN amp.amp_fbclid IS NOT NULL THEN 'has_fbclid'
        WHEN amp.amp_utm_source IS NOT NULL THEN 'has_utm'
        ELSE 'no_attribution'
    END as attribution_type,
    fc.facebook_campaign_id IS NOT NULL as matched_to_campaign,
    COUNT(DISTINCT ss.session_id) as sessions,
    SUM(COALESCE(ss.revenue_usd, 0)) as revenue
FROM stripe_sessions ss
LEFT JOIN amplitude_attribution amp ON ss.session_id = amp.session_id
LEFT JOIN facebook_campaigns fc ON amp.amp_utm_campaign = fc.campaign_name
GROUP BY 1, 2
ORDER BY revenue DESC
"""
result = execute_query(query)
print("Sessions with revenue by attribution type:")
print("-" * 70)
for r in result:
    rev = r['revenue'] if r['revenue'] else 0
    print(f"  {r['attribution_type']}, campaign_match={r['matched_to_campaign']}: {r['sessions']} sessions, ${rev:,.2f}")

# Check what utm_campaign values we have
print()
print("UTM campaigns from Amplitude (sessions with revenue):")
print("-" * 70)
query2 = """
WITH amplitude_with_session AS (
    SELECT
        SUBSTRING(event_properties->>'[Amplitude] Page Location' FROM 'fsid=([A-Z0-9]+)') AS ff_session_id,
        NULLIF(user_properties->>'initial_utm_campaign', 'EMPTY') AS utm_campaign
    FROM raw_amplitude.events
),
stripe_sessions AS (
    SELECT metadata->>'ff_session_id' as session_id
    FROM raw_stripe.subscriptions
    WHERE metadata->>'ff_session_id' IS NOT NULL
)
SELECT DISTINCT amp.utm_campaign
FROM stripe_sessions ss
JOIN amplitude_with_session amp ON ss.session_id = amp.ff_session_id
WHERE amp.utm_campaign IS NOT NULL
LIMIT 20
"""
result2 = execute_query(query2)
for r in result2:
    print(f"  {r['utm_campaign']}")

print()
print("Facebook campaign names (sample):")
print("-" * 70)
query3 = """
SELECT DISTINCT campaign_name
FROM raw_facebook.facebook_campaigns
WHERE campaign_name IS NOT NULL
ORDER BY campaign_name
LIMIT 20
"""
result3 = execute_query(query3)
for r in result3:
    print(f"  {r['campaign_name']}")
