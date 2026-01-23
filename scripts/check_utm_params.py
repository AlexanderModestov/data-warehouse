"""Check UTM params for sessions with revenue."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))
from lib.db import execute_query

print("Sessions with revenue - UTM parameters:")
print("=" * 70)

query = """
SELECT
    session_id,
    utm_source,
    utm_medium,
    utm_campaign,
    fbclid IS NOT NULL as has_fbclid,
    ad_id,
    revenue_usd
FROM analytics.mart_marketing_attribution
WHERE converted = TRUE AND revenue_usd > 0
LIMIT 20
"""
result = execute_query(query)
for r in result:
    print(f"  session={r['session_id'][:20]}...")
    print(f"    utm_source={r['utm_source']}, utm_medium={r['utm_medium']}")
    print(f"    utm_campaign={r['utm_campaign']}")
    print(f"    has_fbclid={r['has_fbclid']}, ad_id={r['ad_id']}")
    print(f"    revenue=${r['revenue_usd']}")
    print()

print("-" * 70)

# Aggregate by utm_source
query2 = """
SELECT
    COALESCE(utm_source, '(none)') as utm_source,
    COALESCE(utm_medium, '(none)') as utm_medium,
    COUNT(*) as sessions,
    SUM(revenue_usd) as revenue
FROM analytics.mart_marketing_attribution
WHERE converted = TRUE AND revenue_usd > 0
GROUP BY utm_source, utm_medium
ORDER BY revenue DESC
"""
result2 = execute_query(query2)
print("Revenue by UTM source/medium:")
for r in result2:
    rev = r['revenue'] if r['revenue'] else 0
    print(f"  {r['utm_source']} / {r['utm_medium']}: {r['sessions']} sessions, ${rev:,.2f}")
