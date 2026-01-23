"""
Compare data between raw_facebook and raw_facebook_new schemas
"""
import sys
import os

# Add dashboard lib to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))

from lib.db import execute_query


def compare_schemas():
    print("=" * 80)
    print("SCHEMA STRUCTURE COMPARISON")
    print("=" * 80)

    # Get all tables and columns for both schemas
    for schema in ['raw_facebook', 'raw_facebook_new']:
        print(f"\n{schema}:")
        print("-" * 40)
        tables = execute_query(f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
            ORDER BY table_name
        """)
        for t in tables:
            cols = execute_query(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = '{schema}' AND table_name = '{t['table_name']}'
                ORDER BY ordinal_position
            """)
            col_names = [c['column_name'] for c in cols]
            count = execute_query(f"SELECT COUNT(*) as cnt FROM {schema}.{t['table_name']}")[0]['cnt']
            print(f"  {t['table_name']} ({count:,} rows)")
            print(f"    Columns: {', '.join(col_names[:10])}{'...' if len(col_names) > 10 else ''}")

    # Compare equivalent tables
    print("\n" + "=" * 80)
    print("EQUIVALENT TABLE MAPPING")
    print("=" * 80)
    print("""
    raw_facebook                    <-->  raw_facebook_new
    -------------------------------------------------------
    facebook_ad_statistics          <-->  adsinsights_default
    facebook_campaigns              <-->  campaigns
    facebook_ads                    <-->  ads
    facebook_adsets                 <-->  adsets
    """)

    # Compare facebook_ad_statistics vs adsinsights_default
    print("\n" + "=" * 80)
    print("COMPARISON: facebook_ad_statistics vs adsinsights_default")
    print("=" * 80)

    old_stats = execute_query("""
        SELECT
            MIN(report_date) as min_date,
            MAX(report_date) as max_date,
            COUNT(*) as rows,
            COUNT(DISTINCT report_date) as unique_dates,
            SUM(amount_spent) as total_spend,
            SUM(impressions) as total_impressions,
            SUM(clicks) as total_clicks
        FROM raw_facebook.facebook_ad_statistics
    """)[0]

    new_stats = execute_query("""
        SELECT
            MIN(date_start::date) as min_date,
            MAX(date_start::date) as max_date,
            COUNT(*) as rows,
            COUNT(DISTINCT date_start::date) as unique_dates,
            SUM(spend::numeric) as total_spend,
            SUM(impressions::bigint) as total_impressions,
            SUM(clicks::bigint) as total_clicks
        FROM raw_facebook_new.adsinsights_default
    """)[0]

    print(f"\n{'Metric':<25} {'raw_facebook':<25} {'raw_facebook_new':<25}")
    print("-" * 75)
    print(f"{'Date range':<25} {str(old_stats['min_date'])} - {str(old_stats['max_date']):<10} {str(new_stats['min_date'])} - {str(new_stats['max_date']):<10}")
    print(f"{'Rows':<25} {old_stats['rows']:>20,} {new_stats['rows']:>20,}")
    print(f"{'Unique dates':<25} {old_stats['unique_dates']:>20,} {new_stats['unique_dates']:>20,}")
    print(f"{'Total spend':<25} ${(old_stats['total_spend'] or 0):>19,.2f} ${(new_stats['total_spend'] or 0):>19,.2f}")
    print(f"{'Total impressions':<25} {(old_stats['total_impressions'] or 0):>20,} {(new_stats['total_impressions'] or 0):>20,}")
    print(f"{'Total clicks':<25} {(old_stats['total_clicks'] or 0):>20,} {(new_stats['total_clicks'] or 0):>20,}")

    # Compare by month
    print("\n" + "=" * 80)
    print("MONTHLY SPEND COMPARISON")
    print("=" * 80)

    monthly_compare = execute_query("""
        WITH old_monthly AS (
            SELECT
                DATE_TRUNC('month', report_date)::date as month,
                SUM(amount_spent) as spend,
                SUM(impressions) as impressions,
                COUNT(*) as rows
            FROM raw_facebook.facebook_ad_statistics
            GROUP BY DATE_TRUNC('month', report_date)
        ),
        new_monthly AS (
            SELECT
                DATE_TRUNC('month', date_start::date)::date as month,
                SUM(spend::numeric) as spend,
                SUM(impressions::bigint) as impressions,
                COUNT(*) as rows
            FROM raw_facebook_new.adsinsights_default
            GROUP BY DATE_TRUNC('month', date_start::date)
        )
        SELECT
            COALESCE(o.month, n.month) as month,
            o.spend as old_spend,
            n.spend as new_spend,
            o.rows as old_rows,
            n.rows as new_rows,
            COALESCE(n.spend, 0) - COALESCE(o.spend, 0) as spend_diff
        FROM old_monthly o
        FULL OUTER JOIN new_monthly n ON o.month = n.month
        ORDER BY month DESC
        LIMIT 12
    """)

    print(f"\n{'Month':<12} {'Old Spend':>15} {'New Spend':>15} {'Difference':>15} {'Old Rows':>10} {'New Rows':>10}")
    print("-" * 80)
    for row in monthly_compare:
        old_s = row['old_spend'] or 0
        new_s = row['new_spend'] or 0
        diff = row['spend_diff'] or 0
        old_r = row['old_rows'] or 0
        new_r = row['new_rows'] or 0
        marker = "" if abs(diff) < 1 else " *"
        print(f"{row['month']}  ${old_s:>13,.2f}  ${new_s:>13,.2f}  ${diff:>13,.2f}{marker}  {old_r:>10,}  {new_r:>10,}")

    # Compare campaigns
    print("\n" + "=" * 80)
    print("CAMPAIGNS COMPARISON")
    print("=" * 80)

    old_camps = execute_query("""
        SELECT COUNT(*) as cnt, COUNT(DISTINCT facebook_campaign_id) as unique_ids
        FROM raw_facebook.facebook_campaigns
    """)[0]

    new_camps = execute_query("""
        SELECT COUNT(*) as cnt, COUNT(DISTINCT id) as unique_ids
        FROM raw_facebook_new.campaigns
    """)[0]

    print(f"\nraw_facebook.facebook_campaigns: {old_camps['cnt']:,} rows, {old_camps['unique_ids']:,} unique campaign IDs")
    print(f"raw_facebook_new.campaigns: {new_camps['cnt']:,} rows, {new_camps['unique_ids']:,} unique campaign IDs")

    # Compare ads
    print("\n" + "=" * 80)
    print("ADS COMPARISON")
    print("=" * 80)

    old_ads = execute_query("""
        SELECT COUNT(*) as cnt, COUNT(DISTINCT facebook_ad_id) as unique_ids
        FROM raw_facebook.facebook_ads
    """)[0]

    new_ads = execute_query("""
        SELECT COUNT(*) as cnt, COUNT(DISTINCT id) as unique_ids
        FROM raw_facebook_new.ads
    """)[0]

    print(f"\nraw_facebook.facebook_ads: {old_ads['cnt']:,} rows, {old_ads['unique_ids']:,} unique ad IDs")
    print(f"raw_facebook_new.ads: {new_ads['cnt']:,} rows, {new_ads['unique_ids']:,} unique ad IDs")

    # Check column differences for adsinsights
    print("\n" + "=" * 80)
    print("COLUMN COMPARISON: facebook_ad_statistics vs adsinsights_default")
    print("=" * 80)

    old_cols = execute_query("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'raw_facebook' AND table_name = 'facebook_ad_statistics'
        ORDER BY ordinal_position
    """)
    old_col_names = set([c['column_name'] for c in old_cols])

    new_cols = execute_query("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'raw_facebook_new' AND table_name = 'adsinsights_default'
        ORDER BY ordinal_position
    """)
    new_col_names = set([c['column_name'] for c in new_cols])

    print(f"\nColumns in raw_facebook.facebook_ad_statistics:")
    print(f"  {sorted(old_col_names)}")

    print(f"\nColumns in raw_facebook_new.adsinsights_default:")
    print(f"  {sorted(new_col_names)}")

    # Common columns (with name mapping)
    print(f"\nColumn mapping analysis:")
    print(f"  report_date -> date_start (date field)")
    print(f"  amount_spent -> spend")
    print(f"  facebook_ad_id -> ad_id")
    print(f"  facebook_campaign_id -> campaign_id")
    print(f"  facebook_adset_id -> adset_id")


if __name__ == '__main__':
    compare_schemas()
