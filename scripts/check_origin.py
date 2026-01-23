"""Check origin field for sessions with revenue."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))
from lib.db import execute_query

print("Sessions with revenue - origin field:")
print("=" * 70)

query = """
SELECT
    s.id as session_id,
    s.origin,
    s.funnel_id
FROM raw_funnelfox.sessions s
INNER JOIN (
    SELECT metadata->>'ff_session_id' as session_id
    FROM raw_stripe.subscriptions
    WHERE metadata->>'ff_session_id' IS NOT NULL
) ss ON s.id = ss.session_id
LIMIT 20
"""
result = execute_query(query)
for r in result:
    origin = r['origin'] if r['origin'] else '(NULL)'
    origin_preview = origin[:100] if len(origin) > 100 else origin
    print(f"  session={r['session_id']}")
    print(f"  funnel_id={r['funnel_id']}")
    print(f"  origin={origin_preview}")
    print()

print("-" * 70)
print()

# Check what origins look like in general
query2 = """
SELECT
    CASE
        WHEN origin IS NULL OR origin = '' THEN '(empty)'
        WHEN origin LIKE '%utm_source=%' THEN 'has_utm'
        WHEN origin LIKE '%fbclid=%' THEN 'has_fbclid'
        WHEN origin LIKE '%ad_id=%' THEN 'has_ad_id'
        ELSE 'other'
    END as origin_type,
    COUNT(*) as sessions
FROM raw_funnelfox.sessions
GROUP BY 1
ORDER BY sessions DESC
"""
result2 = execute_query(query2)
print("Origin field patterns (all sessions):")
for r in result2:
    print(f"  {r['origin_type']}: {r['sessions']:,}")
