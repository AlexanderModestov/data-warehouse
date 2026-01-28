"""
Compare Facebook tables between analytics schema and raw_facebook schema.
Shows main metrics: impressions, clicks, spend, purchases, registrations.
"""

import os
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
import psycopg2
from tabulate import tabulate

# Load environment variables
env_path = Path(__file__).parent.parent / 'meltano' / '.env'
load_dotenv(env_path)

def get_connection():
    return psycopg2.connect(
        host=os.getenv('PG_ANALYTICS_HOST'),
        port=os.getenv('PG_ANALYTICS_PORT', 5432),
        database=os.getenv('PG_ANALYTICS_DBNAME'),
        user=os.getenv('PG_ANALYTICS_USER'),
        password=os.getenv('PG_ANALYTICS_PASSWORD')
    )

def run_query(conn, query):
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            return columns, rows
    except Exception as e:
        conn.rollback()  # Reset transaction on error
        raise e

def main():
    conn = get_connection()

    print("=" * 100)
    print("FACEBOOK TABLES COMPARISON: analytics vs raw_facebook")
    print("=" * 100)

    # 1. List all facebook-related tables in both schemas
    print("\n" + "=" * 100)
    print("1. TABLE INVENTORY")
    print("=" * 100)

    query = """
    SELECT
        table_schema,
        table_name,
        (SELECT COUNT(*) FROM information_schema.columns c
         WHERE c.table_schema = t.table_schema AND c.table_name = t.table_name) as column_count
    FROM information_schema.tables t
    WHERE (table_schema = 'analytics' AND table_name LIKE '%facebook%')
       OR (table_schema = 'raw_facebook')
       OR (table_schema = 'raw_facebook_new')
    ORDER BY table_schema, table_name
    """
    cols, rows = run_query(conn, query)
    print(tabulate(rows, headers=cols, tablefmt='psql'))

    # 2. Row counts for each table
    print("\n" + "=" * 100)
    print("2. ROW COUNTS BY TABLE")
    print("=" * 100)

    tables_to_count = [
        ('analytics', 'stg_facebook_new__ad_statistics'),
        ('analytics', 'stg_facebook_new__campaigns'),
        ('analytics', 'stg_facebook_new__ads'),
        ('analytics', 'stg_facebook_new__adsets'),
        ('raw_facebook', 'facebook_ad_statistics'),
        ('raw_facebook', 'facebook_campaigns'),
        ('raw_facebook', 'facebook_ads'),
        ('raw_facebook', 'facebook_adsets'),
        ('raw_facebook_new', 'adsinsights_default'),
        ('raw_facebook_new', 'campaigns'),
        ('raw_facebook_new', 'ads'),
        ('raw_facebook_new', 'adsets'),
    ]

    count_results = []
    for schema, table in tables_to_count:
        try:
            query = f"SELECT COUNT(*) FROM {schema}.{table}"
            _, rows = run_query(conn, query)
            count_results.append((schema, table, rows[0][0]))
        except Exception as e:
            count_results.append((schema, table, f"ERROR: {str(e)[:50]}"))

    print(tabulate(count_results, headers=['Schema', 'Table', 'Row Count'], tablefmt='psql'))

    # 3. Date ranges for statistics tables
    print("\n" + "=" * 100)
    print("3. DATE RANGES FOR STATISTICS TABLES")
    print("=" * 100)

    date_queries = [
        ("analytics.stg_facebook_new__ad_statistics", "report_date"),
        ("raw_facebook.facebook_ad_statistics", "report_date"),
        ("raw_facebook_new.adsinsights_default", "date_start::date"),
    ]

    date_results = []
    for table, date_col in date_queries:
        try:
            query = f"""
            SELECT
                MIN({date_col}) as min_date,
                MAX({date_col}) as max_date,
                COUNT(DISTINCT {date_col}) as days
            FROM {table}
            """
            _, rows = run_query(conn, query)
            date_results.append((table, rows[0][0], rows[0][1], rows[0][2]))
        except Exception as e:
            date_results.append((table, "ERROR", str(e)[:30], ""))

    print(tabulate(date_results, headers=['Table', 'Min Date', 'Max Date', 'Days'], tablefmt='psql'))

    # 4. MAIN METRICS COMPARISON - Monthly totals
    print("\n" + "=" * 100)
    print("4. MONTHLY METRICS COMPARISON")
    print("=" * 100)

    # From raw_facebook.facebook_ad_statistics
    print("\n--- raw_facebook.facebook_ad_statistics (Monthly) ---")
    query = """
    SELECT
        DATE_TRUNC('month', report_date)::date as month,
        ROUND(SUM(COALESCE(amount_spent, 0))::numeric, 2) as spend,
        SUM(COALESCE(impressions, 0)) as impressions,
        SUM(COALESCE(clicks, 0)) as clicks,
        SUM(COALESCE(purchases, 0)) as purchases,
        SUM(COALESCE(registrations_completed, 0)) as registrations,
        SUM(COALESCE(leads, 0)) as leads
    FROM raw_facebook.facebook_ad_statistics
    GROUP BY DATE_TRUNC('month', report_date)
    ORDER BY month DESC
    LIMIT 12
    """
    try:
        cols, rows = run_query(conn, query)
        print(tabulate(rows, headers=cols, tablefmt='psql', floatfmt='.2f'))
    except Exception as e:
        print(f"Error: {e}")

    # From raw_facebook_new.adsinsights_default - using unnest for jsonb array
    print("\n--- raw_facebook_new.adsinsights_default (Monthly) ---")
    query = """
    WITH actions_unnested AS (
        SELECT
            date_start::date as report_date,
            spend::numeric as amount_spent,
            impressions::int as impressions,
            clicks::int as clicks,
            unnest(actions) as action_item
        FROM raw_facebook_new.adsinsights_default
        WHERE actions IS NOT NULL
    ),
    actions_parsed AS (
        SELECT
            report_date,
            amount_spent,
            impressions,
            clicks,
            action_item->>'action_type' as action_type,
            (action_item->>'value')::numeric as action_value
        FROM actions_unnested
    ),
    daily_stats AS (
        SELECT
            report_date,
            amount_spent,
            impressions,
            clicks
        FROM raw_facebook_new.adsinsights_default
    ),
    purchases AS (
        SELECT report_date, SUM(action_value) as purchases
        FROM actions_parsed
        WHERE action_type IN ('purchase', 'omni_purchase', 'offsite_conversion.fb_pixel_purchase')
        GROUP BY report_date
    ),
    registrations AS (
        SELECT report_date, SUM(action_value) as registrations
        FROM actions_parsed
        WHERE action_type IN ('complete_registration', 'omni_complete_registration', 'offsite_conversion.fb_pixel_complete_registration')
        GROUP BY report_date
    ),
    leads AS (
        SELECT report_date, SUM(action_value) as leads
        FROM actions_parsed
        WHERE action_type IN ('lead', 'omni_lead', 'offsite_conversion.fb_pixel_lead')
        GROUP BY report_date
    ),
    daily_agg AS (
        SELECT
            d.report_date,
            SUM(d.amount_spent) as amount_spent,
            SUM(d.impressions) as impressions,
            SUM(d.clicks) as clicks
        FROM (SELECT DISTINCT date_start::date as report_date, spend::numeric as amount_spent, impressions::int, clicks::int
              FROM raw_facebook_new.adsinsights_default) d
        GROUP BY d.report_date
    )
    SELECT
        DATE_TRUNC('month', da.report_date)::date as month,
        ROUND(SUM(COALESCE(da.amount_spent, 0)), 2) as spend,
        SUM(COALESCE(da.impressions, 0)) as impressions,
        SUM(COALESCE(da.clicks, 0)) as clicks,
        SUM(COALESCE(p.purchases, 0))::int as purchases,
        SUM(COALESCE(r.registrations, 0))::int as registrations,
        SUM(COALESCE(l.leads, 0))::int as leads
    FROM daily_agg da
    LEFT JOIN purchases p ON da.report_date = p.report_date
    LEFT JOIN registrations r ON da.report_date = r.report_date
    LEFT JOIN leads l ON da.report_date = l.report_date
    GROUP BY DATE_TRUNC('month', da.report_date)
    ORDER BY month DESC
    LIMIT 12
    """
    try:
        cols, rows = run_query(conn, query)
        print(tabulate(rows, headers=cols, tablefmt='psql', floatfmt='.2f'))
    except Exception as e:
        print(f"Error: {e}")

    # From analytics staging model
    print("\n--- analytics.stg_facebook_new__ad_statistics (Monthly) ---")
    query = """
    SELECT
        DATE_TRUNC('month', report_date)::date as month,
        ROUND(SUM(COALESCE(amount_spent, 0))::numeric, 2) as spend,
        SUM(COALESCE(impressions, 0)) as impressions,
        SUM(COALESCE(clicks, 0)) as clicks,
        SUM(COALESCE(purchases, 0)) as purchases,
        SUM(COALESCE(registrations_completed, 0)) as registrations,
        SUM(COALESCE(leads, 0)) as leads
    FROM analytics.stg_facebook_new__ad_statistics
    GROUP BY DATE_TRUNC('month', report_date)
    ORDER BY month DESC
    LIMIT 12
    """
    try:
        cols, rows = run_query(conn, query)
        print(tabulate(rows, headers=cols, tablefmt='psql', floatfmt='.2f'))
    except Exception as e:
        print(f"Error: {e}")

    # 5. SIDE-BY-SIDE COMPARISON for overlapping months
    print("\n" + "=" * 100)
    print("5. SIDE-BY-SIDE COMPARISON (Overlapping Months)")
    print("=" * 100)

    query = """
    WITH old_fb AS (
        SELECT
            DATE_TRUNC('month', report_date)::date as month,
            ROUND(SUM(COALESCE(amount_spent, 0))::numeric, 2) as spend,
            SUM(COALESCE(impressions, 0)) as impressions,
            SUM(COALESCE(clicks, 0)) as clicks,
            SUM(COALESCE(purchases, 0)) as purchases,
            SUM(COALESCE(registrations_completed, 0)) as registrations
        FROM raw_facebook.facebook_ad_statistics
        GROUP BY DATE_TRUNC('month', report_date)
    ),
    new_fb AS (
        SELECT
            DATE_TRUNC('month', report_date)::date as month,
            ROUND(SUM(COALESCE(amount_spent, 0))::numeric, 2) as spend,
            SUM(COALESCE(impressions, 0)) as impressions,
            SUM(COALESCE(clicks, 0)) as clicks,
            SUM(COALESCE(purchases, 0)) as purchases,
            SUM(COALESCE(registrations_completed, 0)) as registrations
        FROM analytics.stg_facebook_new__ad_statistics
        GROUP BY DATE_TRUNC('month', report_date)
    )
    SELECT
        COALESCE(o.month, n.month) as month,
        o.spend as old_spend,
        n.spend as new_spend,
        ROUND((COALESCE(n.spend, 0) - COALESCE(o.spend, 0))::numeric, 2) as spend_diff,
        o.impressions as old_impr,
        n.impressions as new_impr,
        o.clicks as old_clicks,
        n.clicks as new_clicks,
        o.purchases as old_purch,
        n.purchases as new_purch,
        o.registrations as old_regs,
        n.registrations as new_regs
    FROM old_fb o
    FULL OUTER JOIN new_fb n ON o.month = n.month
    ORDER BY month DESC
    LIMIT 12
    """
    try:
        cols, rows = run_query(conn, query)
        print(tabulate(rows, headers=cols, tablefmt='psql', floatfmt='.2f'))
    except Exception as e:
        print(f"Error: {e}")

    # 6. DAILY COMPARISON for recent dates
    print("\n" + "=" * 100)
    print("6. DAILY COMPARISON (Last 14 Days)")
    print("=" * 100)

    query = """
    WITH old_fb AS (
        SELECT
            report_date as date,
            ROUND(SUM(COALESCE(amount_spent, 0))::numeric, 2) as spend,
            SUM(COALESCE(impressions, 0)) as impressions,
            SUM(COALESCE(clicks, 0)) as clicks,
            SUM(COALESCE(purchases, 0)) as purchases
        FROM raw_facebook.facebook_ad_statistics
        WHERE report_date >= CURRENT_DATE - INTERVAL '14 days'
        GROUP BY report_date
    ),
    new_fb AS (
        SELECT
            report_date as date,
            ROUND(SUM(COALESCE(amount_spent, 0))::numeric, 2) as spend,
            SUM(COALESCE(impressions, 0)) as impressions,
            SUM(COALESCE(clicks, 0)) as clicks,
            SUM(COALESCE(purchases, 0)) as purchases
        FROM analytics.stg_facebook_new__ad_statistics
        WHERE report_date >= CURRENT_DATE - INTERVAL '14 days'
        GROUP BY report_date
    )
    SELECT
        COALESCE(o.date, n.date) as date,
        o.spend as old_spend,
        n.spend as new_spend,
        ROUND((COALESCE(n.spend, 0) - COALESCE(o.spend, 0))::numeric, 2) as spend_diff,
        o.impressions as old_impr,
        n.impressions as new_impr,
        o.clicks as old_clicks,
        n.clicks as new_clicks,
        o.purchases as old_purch,
        n.purchases as new_purch
    FROM old_fb o
    FULL OUTER JOIN new_fb n ON o.date = n.date
    ORDER BY date DESC
    """
    try:
        cols, rows = run_query(conn, query)
        print(tabulate(rows, headers=cols, tablefmt='psql', floatfmt='.2f'))
    except Exception as e:
        print(f"Error: {e}")

    # 7. ACTION TYPES available in raw_facebook_new
    print("\n" + "=" * 100)
    print("7. ACTION TYPES IN raw_facebook_new.adsinsights_default")
    print("=" * 100)

    query = """
    WITH action_types AS (
        SELECT
            action_item->>'action_type' as action_type,
            SUM((action_item->>'value')::numeric) as total_value
        FROM raw_facebook_new.adsinsights_default,
             unnest(actions) as action_item
        WHERE actions IS NOT NULL
        GROUP BY action_item->>'action_type'
    )
    SELECT action_type, total_value::bigint as total_value
    FROM action_types
    WHERE total_value > 0
    ORDER BY total_value DESC
    LIMIT 30
    """
    try:
        cols, rows = run_query(conn, query)
        print(tabulate(rows, headers=cols, tablefmt='psql'))
    except Exception as e:
        print(f"Error: {e}")

    # 8. Unique ad/campaign counts comparison
    print("\n" + "=" * 100)
    print("8. UNIQUE ENTITY COUNTS")
    print("=" * 100)

    query = """
    SELECT
        'raw_facebook' as source,
        (SELECT COUNT(DISTINCT facebook_ad_id) FROM raw_facebook.facebook_ad_statistics) as unique_ads,
        (SELECT COUNT(DISTINCT facebook_adset_id) FROM raw_facebook.facebook_ad_statistics) as unique_adsets,
        (SELECT COUNT(DISTINCT facebook_campaign_id) FROM raw_facebook.facebook_ad_statistics) as unique_campaigns,
        (SELECT COUNT(*) FROM raw_facebook.facebook_ads) as ads_table,
        (SELECT COUNT(*) FROM raw_facebook.facebook_adsets) as adsets_table,
        (SELECT COUNT(*) FROM raw_facebook.facebook_campaigns) as campaigns_table
    UNION ALL
    SELECT
        'raw_facebook_new' as source,
        (SELECT COUNT(DISTINCT ad_id) FROM raw_facebook_new.adsinsights_default) as unique_ads,
        (SELECT COUNT(DISTINCT adset_id) FROM raw_facebook_new.adsinsights_default) as unique_adsets,
        (SELECT COUNT(DISTINCT campaign_id) FROM raw_facebook_new.adsinsights_default) as unique_campaigns,
        (SELECT COUNT(*) FROM raw_facebook_new.ads) as ads_table,
        (SELECT COUNT(*) FROM raw_facebook_new.adsets) as adsets_table,
        (SELECT COUNT(*) FROM raw_facebook_new.campaigns) as campaigns_table
    UNION ALL
    SELECT
        'analytics (staging)' as source,
        (SELECT COUNT(DISTINCT facebook_ad_id) FROM analytics.stg_facebook_new__ad_statistics) as unique_ads,
        (SELECT COUNT(DISTINCT facebook_adset_id) FROM analytics.stg_facebook_new__ad_statistics) as unique_adsets,
        (SELECT COUNT(DISTINCT facebook_campaign_id) FROM analytics.stg_facebook_new__ad_statistics) as unique_campaigns,
        (SELECT COUNT(*) FROM analytics.stg_facebook_new__ads) as ads_table,
        (SELECT COUNT(*) FROM analytics.stg_facebook_new__adsets) as adsets_table,
        (SELECT COUNT(*) FROM analytics.stg_facebook_new__campaigns) as campaigns_table
    """
    try:
        cols, rows = run_query(conn, query)
        print(tabulate(rows, headers=cols, tablefmt='psql'))
    except Exception as e:
        print(f"Error: {e}")

    # 9. Total metrics summary
    print("\n" + "=" * 100)
    print("9. TOTAL METRICS SUMMARY (ALL TIME)")
    print("=" * 100)

    query = """
    SELECT
        'raw_facebook' as source,
        MIN(report_date) as min_date,
        MAX(report_date) as max_date,
        ROUND(SUM(COALESCE(amount_spent, 0))::numeric, 2) as total_spend,
        SUM(COALESCE(impressions, 0)) as total_impressions,
        SUM(COALESCE(clicks, 0)) as total_clicks,
        SUM(COALESCE(purchases, 0)) as total_purchases,
        SUM(COALESCE(registrations_completed, 0)) as total_registrations,
        SUM(COALESCE(leads, 0)) as total_leads
    FROM raw_facebook.facebook_ad_statistics
    UNION ALL
    SELECT
        'analytics (staging)' as source,
        MIN(report_date) as min_date,
        MAX(report_date) as max_date,
        ROUND(SUM(COALESCE(amount_spent, 0))::numeric, 2) as total_spend,
        SUM(COALESCE(impressions, 0)) as total_impressions,
        SUM(COALESCE(clicks, 0)) as total_clicks,
        SUM(COALESCE(purchases, 0)) as total_purchases,
        SUM(COALESCE(registrations_completed, 0)) as total_registrations,
        SUM(COALESCE(leads, 0)) as total_leads
    FROM analytics.stg_facebook_new__ad_statistics
    ORDER BY source
    """
    try:
        cols, rows = run_query(conn, query)
        print(tabulate(rows, headers=cols, tablefmt='psql', floatfmt='.2f'))
    except Exception as e:
        print(f"Error: {e}")

    # 10. Columns comparison
    print("\n" + "=" * 100)
    print("10. COLUMN COMPARISON - AD STATISTICS")
    print("=" * 100)

    query = """
    WITH old_cols AS (
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'raw_facebook' AND table_name = 'facebook_ad_statistics'
    ),
    new_cols AS (
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'analytics' AND table_name = 'stg_facebook_new__ad_statistics'
    )
    SELECT
        COALESCE(o.column_name, n.column_name) as column_name,
        o.data_type as old_type,
        n.data_type as new_type,
        CASE
            WHEN o.column_name IS NULL THEN 'NEW ONLY'
            WHEN n.column_name IS NULL THEN 'OLD ONLY'
            ELSE 'BOTH'
        END as presence
    FROM old_cols o
    FULL OUTER JOIN new_cols n ON o.column_name = n.column_name
    ORDER BY
        CASE WHEN o.column_name IS NULL THEN 1 WHEN n.column_name IS NULL THEN 2 ELSE 0 END,
        column_name
    """
    try:
        cols, rows = run_query(conn, query)
        print(tabulate(rows, headers=cols, tablefmt='psql'))
    except Exception as e:
        print(f"Error: {e}")

    conn.close()
    print("\n" + "=" * 100)
    print("COMPARISON COMPLETE")
    print("=" * 100)

if __name__ == '__main__':
    main()
