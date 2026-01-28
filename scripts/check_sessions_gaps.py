import psycopg2
from datetime import datetime, timedelta

# Connect to analytics database
conn = psycopg2.connect(
    host="c3bb2s5qilch32.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com",
    user="uefdt0t8idi0oj",
    password="p9619b42d1f2c558de6e71db3449dfcd14e8c05ed5853f6024bfea9c6af3e2e1f",
    dbname="deo269d5mv27qe",
    port="5432"
)
cur = conn.cursor()

# Check session counts by day
query = """
SELECT 
    DATE(created_at) as session_date,
    COUNT(*) as session_count
FROM raw_funnelfox.sessions
WHERE created_at >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY DATE(created_at)
ORDER BY session_date DESC
"""

cur.execute(query)
results = cur.fetchall()

print("=== Session counts by day (last 90 days) ===\n")
print(f"{'Date':<12} {'Sessions':>10}")
print("-" * 24)

dates_with_data = set()
for row in results:
    date_str = row[0].strftime('%Y-%m-%d')
    dates_with_data.add(row[0])
    print(f"{date_str:<12} {row[1]:>10,}")

# Find missing dates
print("\n=== Missing dates (gaps) ===\n")
if results:
    min_date = min(dates_with_data)
    max_date = max(dates_with_data)
    
    all_dates = set()
    current = min_date
    while current <= max_date:
        all_dates.add(current)
        current += timedelta(days=1)
    
    missing_dates = sorted(all_dates - dates_with_data)
    
    if missing_dates:
        for d in missing_dates:
            print(f"MISSING: {d.strftime('%Y-%m-%d')}")
    else:
        print("No gaps found - all dates have data")

cur.close()
conn.close()
