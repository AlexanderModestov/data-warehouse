"""Check Amplitude attribution data and ff_session_id linkage."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))
from lib.db import execute_query

print("Amplitude attribution data")
print("=" * 70)

# Check user_properties for attribution
query1 = """
SELECT
    user_id,
    user_properties->>'initial_utm_source' as utm_source,
    user_properties->>'initial_utm_medium' as utm_medium,
    user_properties->>'initial_utm_campaign' as utm_campaign,
    user_properties->>'initial_fbclid' as fbclid,
    SUBSTRING(event_properties->>'[Amplitude] Page Location' FROM 'fsid=([A-Z0-9]+)') as ff_session_id
FROM raw_amplitude.events
WHERE user_properties->>'initial_utm_source' IS NOT NULL
   OR user_properties->>'initial_fbclid' IS NOT NULL
LIMIT 10
"""
result1 = execute_query(query1)
print("Sample Amplitude events with attribution:")
print("-" * 70)
for r in result1:
    print(f"  user_id={r['user_id']}")
    print(f"    utm: {r['utm_source']} / {r['utm_medium']} / {r['utm_campaign']}")
    print(f"    fbclid={r['fbclid'][:30] if r['fbclid'] else None}...")
    print(f"    ff_session_id={r['ff_session_id']}")
    print()

# Check how many events have attribution + ff_session_id
print()
print("Attribution coverage:")
print("-" * 70)
query2 = """
SELECT
    COUNT(*) as total_events,
    COUNT(user_properties->>'initial_utm_source') as has_utm_source,
    COUNT(user_properties->>'initial_fbclid') as has_fbclid,
    COUNT(SUBSTRING(event_properties->>'[Amplitude] Page Location' FROM 'fsid=([A-Z0-9]+)')) as has_ff_session_id,
    COUNT(DISTINCT SUBSTRING(event_properties->>'[Amplitude] Page Location' FROM 'fsid=([A-Z0-9]+)')) as unique_sessions
FROM raw_amplitude.events
"""
result2 = execute_query(query2)
for r in result2:
    print(f"  Total events:        {r['total_events']:,}")
    print(f"  Has utm_source:      {r['has_utm_source']:,}")
    print(f"  Has fbclid:          {r['has_fbclid']:,}")
    print(f"  Has ff_session_id:   {r['has_ff_session_id']:,}")
    print(f"  Unique sessions:     {r['unique_sessions']:,}")

# Check if we can link to sessions with revenue
print()
print("Sessions with revenue + Amplitude attribution:")
print("-" * 70)
query3 = """
WITH amplitude_sessions AS (
    SELECT DISTINCT
        SUBSTRING(event_properties->>'[Amplitude] Page Location' FROM 'fsid=([A-Z0-9]+)') as ff_session_id,
        FIRST_VALUE(user_properties->>'initial_utm_source') OVER (
            PARTITION BY SUBSTRING(event_properties->>'[Amplitude] Page Location' FROM 'fsid=([A-Z0-9]+)')
            ORDER BY event_time
        ) as utm_source,
        FIRST_VALUE(user_properties->>'initial_utm_campaign') OVER (
            PARTITION BY SUBSTRING(event_properties->>'[Amplitude] Page Location' FROM 'fsid=([A-Z0-9]+)')
            ORDER BY event_time
        ) as utm_campaign,
        FIRST_VALUE(user_properties->>'initial_fbclid') OVER (
            PARTITION BY SUBSTRING(event_properties->>'[Amplitude] Page Location' FROM 'fsid=([A-Z0-9]+)')
            ORDER BY event_time
        ) as fbclid
    FROM raw_amplitude.events
    WHERE SUBSTRING(event_properties->>'[Amplitude] Page Location' FROM 'fsid=([A-Z0-9]+)') IS NOT NULL
),
stripe_sessions AS (
    SELECT
        metadata->>'ff_session_id' as ff_session_id
    FROM raw_stripe.subscriptions
    WHERE metadata->>'ff_session_id' IS NOT NULL
)
SELECT
    COUNT(DISTINCT ss.ff_session_id) as sessions_with_revenue,
    COUNT(DISTINCT CASE WHEN amp.utm_source IS NOT NULL THEN ss.ff_session_id END) as has_utm,
    COUNT(DISTINCT CASE WHEN amp.fbclid IS NOT NULL THEN ss.ff_session_id END) as has_fbclid
FROM stripe_sessions ss
LEFT JOIN amplitude_sessions amp ON ss.ff_session_id = amp.ff_session_id
"""
result3 = execute_query(query3)
for r in result3:
    print(f"  Sessions with revenue:     {r['sessions_with_revenue']}")
    print(f"  Has UTM attribution:       {r['has_utm']}")
    print(f"  Has fbclid:                {r['has_fbclid']}")
