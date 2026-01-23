"""Check if Amplitude utm_campaign matches Facebook ad IDs."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))
from lib.db import execute_query

print("Checking Amplitude utm_campaign vs Facebook ad IDs")
print("=" * 70)

# Sample utm_campaign values from Amplitude (for sessions with revenue)
query1 = """
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
LIMIT 10
"""
result1 = execute_query(query1)
utm_campaigns = [r['utm_campaign'] for r in result1]
print("Amplitude utm_campaign values (sessions with revenue):")
for c in utm_campaigns:
    print(f"  {c}")

# Sample facebook_ad_id values
print()
print("Facebook ad IDs (sample):")
query2 = """
SELECT DISTINCT facebook_ad_id
FROM raw_facebook.facebook_ads
WHERE facebook_ad_id IS NOT NULL
LIMIT 10
"""
result2 = execute_query(query2)
fb_ad_ids = [r['facebook_ad_id'] for r in result2]
for aid in fb_ad_ids:
    print(f"  {aid}")

# Check format differences
print()
print("Format comparison:")
print("-" * 70)
if utm_campaigns and fb_ad_ids:
    print(f"  Amplitude utm_campaign example: '{utm_campaigns[0]}' (len={len(utm_campaigns[0])})")
    print(f"  Facebook ad_id example:         '{fb_ad_ids[0]}' (len={len(fb_ad_ids[0])})")

# Direct check - do any match?
print()
print("Direct overlap check:")
query3 = """
SELECT COUNT(*) as matching_count
FROM (
    SELECT DISTINCT NULLIF(user_properties->>'initial_utm_campaign', 'EMPTY') AS utm_campaign
    FROM raw_amplitude.events
    WHERE user_properties->>'initial_utm_campaign' IS NOT NULL
      AND user_properties->>'initial_utm_campaign' != 'EMPTY'
) amp
JOIN raw_facebook.facebook_ads fb ON amp.utm_campaign = fb.facebook_ad_id
"""
result3 = execute_query(query3)
print(f"  Matching utm_campaign -> facebook_ad_id: {result3[0]['matching_count']}")

# Check if utm_campaign might be facebook_campaign_id instead
print()
print("Check if utm_campaign matches facebook_campaign_id:")
query4 = """
SELECT COUNT(*) as matching_count
FROM (
    SELECT DISTINCT NULLIF(user_properties->>'initial_utm_campaign', 'EMPTY') AS utm_campaign
    FROM raw_amplitude.events
    WHERE user_properties->>'initial_utm_campaign' IS NOT NULL
      AND user_properties->>'initial_utm_campaign' != 'EMPTY'
) amp
JOIN raw_facebook.facebook_campaigns fc ON amp.utm_campaign = fc.facebook_campaign_id
"""
result4 = execute_query(query4)
print(f"  Matching utm_campaign -> facebook_campaign_id: {result4[0]['matching_count']}")
