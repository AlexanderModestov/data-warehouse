"""Check if sessions with revenue have facebook_campaign_id."""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))
from lib.db import execute_query

print("Checking if sessions with revenue have facebook_campaign_id")
print("=" * 70)

# Check mart_marketing_attribution for revenue and campaign linkage
query = """
SELECT
    COUNT(*) as total_sessions,
    SUM(CASE WHEN converted THEN 1 ELSE 0 END) as conversions,
    SUM(COALESCE(revenue_usd, 0)) as total_revenue,
    COUNT(facebook_campaign_id) as has_campaign_id,
    COUNT(facebook_ad_id) as has_ad_id
FROM analytics.mart_marketing_attribution
"""
result = execute_query(query)
print("mart_marketing_attribution stats:")
for r in result:
    print(f"  Total sessions:      {r['total_sessions']:,}")
    print(f"  Conversions:         {r['conversions']:,}")
    rev = r['total_revenue'] if r['total_revenue'] else 0
    print(f"  Total revenue:       ${rev:,.2f}")
    print(f"  Has campaign_id:     {r['has_campaign_id']:,}")
    print(f"  Has ad_id:           {r['has_ad_id']:,}")

print()
print("-" * 70)

# Check sessions WITH revenue - do they have campaign_id?
query2 = """
SELECT
    facebook_campaign_id IS NOT NULL as has_campaign,
    facebook_ad_id IS NOT NULL as has_ad,
    COUNT(*) as sessions,
    SUM(revenue_usd) as revenue
FROM analytics.mart_marketing_attribution
WHERE converted = TRUE AND revenue_usd > 0
GROUP BY 1, 2
ORDER BY revenue DESC
"""
result2 = execute_query(query2)
print("Sessions WITH revenue - campaign linkage:")
for r in result2:
    rev = r['revenue'] if r['revenue'] else 0
    print(f"  has_campaign={r['has_campaign']}, has_ad={r['has_ad']}: {r['sessions']} sessions, ${rev:,.2f}")

print()
print("-" * 70)

# Check mart_marketing_performance
query3 = """
SELECT
    COUNT(*) as rows,
    SUM(revenue_usd) as total_revenue,
    SUM(attributed_conversions) as conversions
FROM analytics.mart_marketing_performance
"""
result3 = execute_query(query3)
print("mart_marketing_performance stats:")
for r in result3:
    print(f"  Rows:          {r['rows']:,}")
    rev = r['total_revenue'] if r['total_revenue'] else 0
    print(f"  Revenue:       ${rev:,.2f}")
    conv = r['conversions'] if r['conversions'] else 0
    print(f"  Conversions:   {conv:,}")

print()
print("=" * 70)
