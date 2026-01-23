"""
Diagnostic script to compare amount_spent in raw_facebook vs spend_usd in mart.
"""
import sys
import os

# Add dashboard lib to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))

from lib.db import execute_query


def main():
    print("=" * 70)
    print("SPEND COMPARISON: raw_facebook vs mart_marketing_performance")
    print("=" * 70)

    # 1. Overall totals
    print("\n1. OVERALL TOTALS:")
    print("-" * 70)

    query_raw = """
        SELECT
            SUM(amount_spent) AS total_spend,
            COUNT(*) AS row_count,
            COUNT(DISTINCT facebook_ad_id) AS unique_ads,
            COUNT(DISTINCT report_date) AS unique_dates
        FROM raw_facebook.facebook_ad_statistics
    """
    raw_result = execute_query(query_raw)[0]

    query_mart = """
        SELECT
            SUM(spend_usd) AS total_spend,
            COUNT(*) AS row_count,
            COUNT(DISTINCT facebook_ad_id) AS unique_ads,
            COUNT(DISTINCT date) AS unique_dates
        FROM analytics.mart_marketing_performance
    """
    mart_result = execute_query(query_mart)[0]

    raw_spend = raw_result['total_spend'] or 0
    mart_spend = mart_result['total_spend'] or 0

    print(f"  {'Source':<40} {'Spend':>15} {'Rows':>10} {'Ads':>10}")
    print(f"  {'-'*40} {'-'*15} {'-'*10} {'-'*10}")
    print(f"  {'raw_facebook.facebook_ad_statistics':<40} ${raw_spend:>14,.2f} {raw_result['row_count']:>10,} {raw_result['unique_ads']:>10,}")
    print(f"  {'analytics.mart_marketing_performance':<40} ${mart_spend:>14,.2f} {mart_result['row_count']:>10,} {mart_result['unique_ads']:>10,}")
    print(f"  {'-'*40} {'-'*15} {'-'*10} {'-'*10}")
    print(f"  {'DIFFERENCE':<40} ${raw_spend - mart_spend:>14,.2f} {raw_result['row_count'] - mart_result['row_count']:>10,} {raw_result['unique_ads'] - mart_result['unique_ads']:>10,}")

    # 2. Check for NULL filters
    print("\n2. ROWS EXCLUDED BY FILTERS:")
    print("-" * 70)

    query_nulls = """
        SELECT
            SUM(CASE WHEN facebook_ad_id IS NULL THEN 1 ELSE 0 END) AS null_ad_id_rows,
            SUM(CASE WHEN facebook_ad_id IS NULL THEN amount_spent ELSE 0 END) AS null_ad_id_spend,
            SUM(CASE WHEN report_date IS NULL THEN 1 ELSE 0 END) AS null_date_rows,
            SUM(CASE WHEN report_date IS NULL THEN amount_spent ELSE 0 END) AS null_date_spend,
            SUM(CASE WHEN facebook_ad_id IS NULL OR report_date IS NULL THEN 1 ELSE 0 END) AS any_null_rows,
            SUM(CASE WHEN facebook_ad_id IS NULL OR report_date IS NULL THEN amount_spent ELSE 0 END) AS any_null_spend
        FROM raw_facebook.facebook_ad_statistics
    """
    null_result = execute_query(query_nulls)[0]

    print(f"  Rows with NULL facebook_ad_id: {null_result['null_ad_id_rows']:,} (${null_result['null_ad_id_spend'] or 0:,.2f})")
    print(f"  Rows with NULL report_date:    {null_result['null_date_rows']:,} (${null_result['null_date_spend'] or 0:,.2f})")
    print(f"  Rows with ANY NULL:            {null_result['any_null_rows']:,} (${null_result['any_null_spend'] or 0:,.2f})")

    # 3. Check aggregation (mart groups by date + campaign + adset + ad)
    print("\n3. AGGREGATION CHECK (raw may have multiple rows per ad/date):")
    print("-" * 70)

    query_agg = """
        SELECT
            COUNT(*) AS total_raw_rows,
            COUNT(DISTINCT (report_date, facebook_campaign_id, facebook_adset_id, facebook_ad_id)) AS unique_combinations
        FROM raw_facebook.facebook_ad_statistics
        WHERE report_date IS NOT NULL
          AND facebook_ad_id IS NOT NULL
    """
    agg_result = execute_query(query_agg)[0]
    print(f"  Raw rows (after NULL filter):  {agg_result['total_raw_rows']:,}")
    print(f"  Unique date+campaign+adset+ad: {agg_result['unique_combinations']:,}")
    print(f"  Duplicates being summed:       {agg_result['total_raw_rows'] - agg_result['unique_combinations']:,}")

    # 4. Daily comparison - find dates with differences
    print("\n4. DAILY DIFFERENCES (top 10 by discrepancy):")
    print("-" * 70)

    query_daily = """
        WITH raw_daily AS (
            SELECT
                report_date AS date,
                SUM(amount_spent) AS spend
            FROM raw_facebook.facebook_ad_statistics
            WHERE report_date IS NOT NULL
              AND facebook_ad_id IS NOT NULL
            GROUP BY report_date
        ),
        mart_daily AS (
            SELECT
                date,
                SUM(spend_usd) AS spend
            FROM analytics.mart_marketing_performance
            GROUP BY date
        )
        SELECT
            COALESCE(r.date, m.date) AS date,
            COALESCE(r.spend, 0) AS raw_spend,
            COALESCE(m.spend, 0) AS mart_spend,
            COALESCE(r.spend, 0) - COALESCE(m.spend, 0) AS difference
        FROM raw_daily r
        FULL OUTER JOIN mart_daily m ON r.date = m.date
        ORDER BY ABS(COALESCE(r.spend, 0) - COALESCE(m.spend, 0)) DESC
        LIMIT 10
    """
    daily_result = execute_query(query_daily)

    print(f"  {'Date':<15} {'Raw Spend':>15} {'Mart Spend':>15} {'Difference':>15}")
    print(f"  {'-'*15} {'-'*15} {'-'*15} {'-'*15}")
    for row in daily_result:
        diff = row['difference'] or 0
        if abs(diff) > 0.01:
            print(f"  {str(row['date']):<15} ${row['raw_spend']:>14,.2f} ${row['mart_spend']:>14,.2f} ${diff:>14,.2f}")
        else:
            print(f"  {str(row['date']):<15} ${row['raw_spend']:>14,.2f} ${row['mart_spend']:>14,.2f} {'OK':>15}")

    # 5. Check if campaign grouping causes issues
    print("\n5. CAMPAIGN NAME VARIATIONS (same campaign_id, different names):")
    print("-" * 70)

    query_names = """
        SELECT
            facebook_campaign_id,
            COUNT(DISTINCT campaign_name) AS name_variations,
            array_agg(DISTINCT campaign_name) AS names
        FROM raw_facebook.facebook_ad_statistics
        WHERE facebook_campaign_id IS NOT NULL
        GROUP BY facebook_campaign_id
        HAVING COUNT(DISTINCT campaign_name) > 1
        LIMIT 5
    """
    name_result = execute_query(query_names)

    if name_result:
        for row in name_result:
            print(f"  Campaign {row['facebook_campaign_id']}: {row['name_variations']} variations")
            print(f"    Names: {row['names'][:3]}...")  # Show first 3
    else:
        print("  No campaign name variations found")

    # 6. Sample rows to check data
    print("\n6. SAMPLE RAW DATA (first 5 rows):")
    print("-" * 70)

    query_sample = """
        SELECT
            report_date,
            facebook_ad_id,
            campaign_name,
            amount_spent
        FROM raw_facebook.facebook_ad_statistics
        ORDER BY report_date DESC
        LIMIT 5
    """
    sample_result = execute_query(query_sample)

    for row in sample_result:
        print(f"  {row['report_date']} | {row['facebook_ad_id']} | ${row['amount_spent'] or 0:.2f} | {row['campaign_name'][:30] if row['campaign_name'] else 'NULL'}...")

    print("\n" + "=" * 70)
    print("DIAGNOSIS COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
