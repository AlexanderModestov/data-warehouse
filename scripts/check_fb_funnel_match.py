"""Check if we can match Facebook ads to FunnelFox funnels."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))
from lib.db import execute_query

print("Checking Facebook -> Funnel matching possibilities")
print("=" * 70)

# Check funnel names/titles
print("FunnelFox funnels:")
print("-" * 70)
query1 = """
SELECT id, title, type, environment
FROM raw_funnelfox.funnels
ORDER BY title
"""
result1 = execute_query(query1)
for r in result1:
    print(f"  {r['title']} (id={r['id'][:20]}..., type={r['type']}, env={r['environment']})")

print()
print("Facebook campaigns:")
print("-" * 70)
query2 = """
SELECT DISTINCT campaign_name, facebook_campaign_id
FROM raw_facebook.facebook_campaigns
WHERE campaign_name IS NOT NULL
ORDER BY campaign_name
LIMIT 20
"""
result2 = execute_query(query2)
for r in result2:
    print(f"  {r['campaign_name']} (id={r['facebook_campaign_id']})")

print()
print("Facebook ad names:")
print("-" * 70)
query3 = """
SELECT DISTINCT ad_name, facebook_ad_id
FROM raw_facebook.facebook_ads
WHERE ad_name IS NOT NULL
ORDER BY ad_name
LIMIT 20
"""
result3 = execute_query(query3)
for r in result3:
    print(f"  {r['ad_name']} (id={r['facebook_ad_id']})")

# Check if there's URL tracking in Facebook ads
print()
print("Checking if Facebook has destination URLs or tracking:")
print("-" * 70)
query4 = """
SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'raw_facebook'
  AND table_name = 'facebook_ads'
ORDER BY ordinal_position
"""
result4 = execute_query(query4)
print("Columns in facebook_ads:")
for r in result4:
    print(f"  - {r['column_name']}")
