"""Quick script to verify impressions data - comparing raw vs mart"""
import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg2

# Load .env from project root
load_dotenv(Path(__file__).parent.parent / '.env')

# Connect to database
conn = psycopg2.connect(
    host=os.getenv('PG_ANALYTICS_HOST'),
    port=os.getenv('PG_ANALYTICS_PORT', '5432'),
    dbname=os.getenv('PG_ANALYTICS_DBNAME'),
    user=os.getenv('PG_ANALYTICS_USER'),
    password=os.getenv('PG_ANALYTICS_PASSWORD')
)

# Target ad names (with fuzzy matching)
target_ads = [
    '01_102_New_Breakup_FB_En_Oneself_Storytell_Motion_9x16',
    '04_99_New_Breakup_FB_En_readyTT_UGC_Motion',
    '02_102_New_Breakup_FB_En_Oneself_Storytell_Motion_9x16',
    '02_101_New_FB_En_Oneself_Storytell_Motion_9x16',
    '04_120_New_Breakup_FB_En_momsaid_storytell_Motion_9x16',
    '01_99_New_Breakup_FB_En_readyTT_UGC_Motion',
    '04_101_New_FB_En_Oneself_Storytell_Motion_9x16',
]

# Query 1: Raw facebook_ad_statistics
print("=" * 80)
print("1. RAW SOURCE: raw_facebook.facebook_ad_statistics")
print("=" * 80)

raw_query = """
SELECT
    a.ad_name,
    s.report_date,
    SUM(s.impressions) as impressions
FROM raw_facebook.facebook_ad_statistics s
JOIN raw_facebook.facebook_ads a ON s.facebook_ad_id = a.facebook_ad_id
WHERE s.report_date >= '2026-01-16'
  AND s.report_date <= '2026-01-20'
  AND a.ad_name LIKE ANY(ARRAY['%%102%%Oneself%%', '%%99%%readyTT%%', '%%101%%Oneself%%', '%%120%%said%%'])
GROUP BY a.ad_name, s.report_date
ORDER BY a.ad_name, s.report_date
"""

with conn.cursor() as cur:
    cur.execute(raw_query)
    rows = cur.fetchall()

print(f"{'Ad Name':<55} | {'Date':<12} | {'Impressions':>10}")
print("-" * 82)
for row in rows:
    print(f"{row[0]:<55} | {row[1]} | {row[2]:>10,}")

# Query 2: Totals comparison
print("\n" + "=" * 80)
print("2. TOTALS: Raw vs Mart (Jan 16-20)")
print("=" * 80)

comparison_query = """
WITH raw_totals AS (
    SELECT
        a.ad_name,
        SUM(s.impressions) as raw_impressions
    FROM raw_facebook.facebook_ad_statistics s
    JOIN raw_facebook.facebook_ads a ON s.facebook_ad_id = a.facebook_ad_id
    WHERE s.report_date >= '2026-01-16'
      AND s.report_date <= '2026-01-20'
      AND a.ad_name LIKE ANY(ARRAY['%%102%%Oneself%%', '%%99%%readyTT%%', '%%101%%Oneself%%Storytell%%', '%%120%%said%%'])
    GROUP BY a.ad_name
),
mart_totals AS (
    SELECT
        ad_name,
        SUM(impressions) as mart_impressions
    FROM analytics.mart_marketing_performance
    WHERE date >= '2026-01-16'
      AND date <= '2026-01-20'
      AND ad_name LIKE ANY(ARRAY['%%102%%Oneself%%', '%%99%%readyTT%%', '%%101%%Oneself%%Storytell%%', '%%120%%said%%'])
    GROUP BY ad_name
)
SELECT
    COALESCE(r.ad_name, m.ad_name) as ad_name,
    COALESCE(r.raw_impressions, 0) as raw_impressions,
    COALESCE(m.mart_impressions, 0) as mart_impressions,
    COALESCE(r.raw_impressions, 0) - COALESCE(m.mart_impressions, 0) as diff
FROM raw_totals r
FULL OUTER JOIN mart_totals m ON r.ad_name = m.ad_name
ORDER BY ad_name
"""

with conn.cursor() as cur:
    cur.execute(comparison_query)
    rows = cur.fetchall()

print(f"{'Ad Name':<55} | {'Raw':>8} | {'Mart':>8} | {'Diff':>8}")
print("-" * 86)
for row in rows:
    print(f"{row[0]:<55} | {row[1]:>8,} | {row[2]:>8,} | {row[3]:>8,}")

# Query 3: Check for duplicate dates or missing joins
print("\n" + "=" * 80)
print("3. RAW DATA: Daily breakdown for specific ads")
print("=" * 80)

daily_query = """
SELECT
    a.ad_name,
    s.report_date,
    s.facebook_ad_id,
    s.impressions,
    s.amount_spent
FROM raw_facebook.facebook_ad_statistics s
JOIN raw_facebook.facebook_ads a ON s.facebook_ad_id = a.facebook_ad_id
WHERE s.report_date >= '2026-01-16'
  AND s.report_date <= '2026-01-20'
  AND a.ad_name LIKE '%%01_102%%Oneself%%'
ORDER BY s.report_date
"""

with conn.cursor() as cur:
    cur.execute(daily_query)
    rows = cur.fetchall()

print(f"{'Ad Name':<50} | {'Date':<12} | {'Ad ID':<20} | {'Impr':>8} | {'Spend':>8}")
print("-" * 105)
for row in rows:
    print(f"{row[0]:<50} | {row[1]} | {row[2]:<20} | {row[3]:>8,} | ${row[4]:>7.2f}")

# Query 4: Check date coverage in raw data
print("\n" + "=" * 80)
print("4. DATE COVERAGE: Which dates have data?")
print("=" * 80)

date_query = """
SELECT
    report_date,
    COUNT(DISTINCT facebook_ad_id) as ad_count,
    SUM(impressions) as total_impressions
FROM raw_facebook.facebook_ad_statistics
WHERE report_date >= '2026-01-16'
  AND report_date <= '2026-01-20'
GROUP BY report_date
ORDER BY report_date
"""

with conn.cursor() as cur:
    cur.execute(date_query)
    rows = cur.fetchall()

print(f"{'Date':<12} | {'Ads':>8} | {'Impressions':>15}")
print("-" * 40)
for row in rows:
    print(f"{row[0]} | {row[1]:>8,} | {row[2]:>15,}")

# Check when last sync happened
print("\n" + "=" * 80)
print("5. LATEST DATA: Most recent report_date in raw_facebook")
print("=" * 80)

latest_query = """
SELECT MAX(report_date) as latest_date
FROM raw_facebook.facebook_ad_statistics
"""

with conn.cursor() as cur:
    cur.execute(latest_query)
    row = cur.fetchone()

print(f"Latest report_date: {row[0]}")

conn.close()
