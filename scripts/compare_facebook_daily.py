"""
Daily comparison between raw_facebook and analytics staging model
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
import psycopg2
from tabulate import tabulate

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
        conn.rollback()
        raise e

def main():
    conn = get_connection()

    print("=" * 140)
    print("DAILY COMPARISON: raw_facebook vs analytics.stg_facebook_new__ad_statistics")
    print("=" * 140)

    # Full daily comparison
    print("\n" + "=" * 140)
    print("FULL DAILY METRICS COMPARISON (All overlapping dates)")
    print("=" * 140)

    query = """
    WITH old_fb AS (
        SELECT
            report_date as date,
            ROUND(SUM(COALESCE(amount_spent, 0))::numeric, 2) as spend,
            SUM(COALESCE(impressions, 0)) as impressions,
            SUM(COALESCE(clicks, 0)) as clicks,
            SUM(COALESCE(purchases, 0)) as purchases,
            SUM(COALESCE(registrations_completed, 0)) as registrations,
            SUM(COALESCE(leads, 0)) as leads
        FROM raw_facebook.facebook_ad_statistics
        GROUP BY report_date
    ),
    new_fb AS (
        SELECT
            report_date as date,
            ROUND(SUM(COALESCE(amount_spent, 0))::numeric, 2) as spend,
            SUM(COALESCE(impressions, 0)) as impressions,
            SUM(COALESCE(clicks, 0)) as clicks,
            SUM(COALESCE(purchases, 0)) as purchases,
            SUM(COALESCE(registrations_completed, 0)) as registrations,
            SUM(COALESCE(leads, 0)) as leads
        FROM analytics.stg_facebook_new__ad_statistics
        GROUP BY report_date
    )
    SELECT
        COALESCE(o.date, n.date) as date,
        o.spend as old_spend,
        n.spend as new_spend,
        ROUND((COALESCE(n.spend, 0) - COALESCE(o.spend, 0))::numeric, 2) as spend_diff,
        o.impressions as old_impr,
        n.impressions as new_impr,
        (COALESCE(n.impressions, 0) - COALESCE(o.impressions, 0)) as impr_diff,
        o.clicks as old_clicks,
        n.clicks as new_clicks,
        (COALESCE(n.clicks, 0) - COALESCE(o.clicks, 0)) as clicks_diff,
        o.purchases as old_purch,
        n.purchases as new_purch,
        (COALESCE(n.purchases, 0) - COALESCE(o.purchases, 0)) as purch_diff,
        o.registrations as old_regs,
        n.registrations as new_regs,
        (COALESCE(n.registrations, 0) - COALESCE(o.registrations, 0)) as regs_diff,
        o.leads as old_leads,
        n.leads as new_leads
    FROM old_fb o
    FULL OUTER JOIN new_fb n ON o.date = n.date
    WHERE o.date IS NOT NULL OR n.date >= '2025-12-20'
    ORDER BY date DESC
    """
    cols, rows = run_query(conn, query)
    print(tabulate(rows, headers=cols, tablefmt='psql', floatfmt='.2f'))

    # Summary statistics
    print("\n" + "=" * 140)
    print("SUMMARY: Differences on overlapping dates")
    print("=" * 140)

    query = """
    WITH old_fb AS (
        SELECT
            report_date as date,
            SUM(COALESCE(amount_spent, 0))::numeric as spend,
            SUM(COALESCE(impressions, 0)) as impressions,
            SUM(COALESCE(clicks, 0)) as clicks,
            SUM(COALESCE(purchases, 0)) as purchases,
            SUM(COALESCE(registrations_completed, 0)) as registrations,
            SUM(COALESCE(leads, 0)) as leads
        FROM raw_facebook.facebook_ad_statistics
        GROUP BY report_date
    ),
    new_fb AS (
        SELECT
            report_date as date,
            SUM(COALESCE(amount_spent, 0))::numeric as spend,
            SUM(COALESCE(impressions, 0)) as impressions,
            SUM(COALESCE(clicks, 0)) as clicks,
            SUM(COALESCE(purchases, 0)) as purchases,
            SUM(COALESCE(registrations_completed, 0)) as registrations,
            SUM(COALESCE(leads, 0)) as leads
        FROM analytics.stg_facebook_new__ad_statistics
        GROUP BY report_date
    ),
    comparison AS (
        SELECT
            o.date,
            o.spend as old_spend,
            n.spend as new_spend,
            ABS(COALESCE(n.spend, 0) - COALESCE(o.spend, 0)) as spend_diff,
            o.impressions as old_impr,
            n.impressions as new_impr,
            ABS(COALESCE(n.impressions, 0) - COALESCE(o.impressions, 0)) as impr_diff,
            o.clicks as old_clicks,
            n.clicks as new_clicks,
            ABS(COALESCE(n.clicks, 0) - COALESCE(o.clicks, 0)) as clicks_diff,
            o.purchases as old_purch,
            n.purchases as new_purch,
            ABS(COALESCE(n.purchases, 0) - COALESCE(o.purchases, 0)) as purch_diff,
            o.registrations as old_regs,
            n.registrations as new_regs,
            ABS(COALESCE(n.registrations, 0) - COALESCE(o.registrations, 0)) as regs_diff
        FROM old_fb o
        INNER JOIN new_fb n ON o.date = n.date
    )
    SELECT
        COUNT(*) as overlapping_days,
        ROUND(SUM(old_spend), 2) as total_old_spend,
        ROUND(SUM(new_spend), 2) as total_new_spend,
        ROUND(SUM(new_spend) - SUM(old_spend), 2) as total_spend_diff,
        SUM(old_impr) as total_old_impr,
        SUM(new_impr) as total_new_impr,
        SUM(new_impr) - SUM(old_impr) as total_impr_diff,
        SUM(old_clicks) as total_old_clicks,
        SUM(new_clicks) as total_new_clicks,
        SUM(new_clicks) - SUM(old_clicks) as total_clicks_diff,
        SUM(old_purch) as total_old_purch,
        SUM(new_purch) as total_new_purch,
        SUM(new_purch) - SUM(old_purch) as total_purch_diff,
        SUM(old_regs) as total_old_regs,
        SUM(new_regs) as total_new_regs,
        SUM(new_regs) - SUM(old_regs) as total_regs_diff
    FROM comparison
    """
    cols, rows = run_query(conn, query)
    print(tabulate(rows, headers=cols, tablefmt='psql', floatfmt='.2f'))

    # Days with differences
    print("\n" + "=" * 140)
    print("DAYS WITH DIFFERENCES (purchases or registrations mismatch)")
    print("=" * 140)

    query = """
    WITH old_fb AS (
        SELECT
            report_date as date,
            ROUND(SUM(COALESCE(amount_spent, 0))::numeric, 2) as spend,
            SUM(COALESCE(impressions, 0)) as impressions,
            SUM(COALESCE(clicks, 0)) as clicks,
            SUM(COALESCE(purchases, 0)) as purchases,
            SUM(COALESCE(registrations_completed, 0)) as registrations
        FROM raw_facebook.facebook_ad_statistics
        GROUP BY report_date
    ),
    new_fb AS (
        SELECT
            report_date as date,
            ROUND(SUM(COALESCE(amount_spent, 0))::numeric, 2) as spend,
            SUM(COALESCE(impressions, 0)) as impressions,
            SUM(COALESCE(clicks, 0)) as clicks,
            SUM(COALESCE(purchases, 0)) as purchases,
            SUM(COALESCE(registrations_completed, 0)) as registrations
        FROM analytics.stg_facebook_new__ad_statistics
        GROUP BY report_date
    )
    SELECT
        o.date,
        o.spend as old_spend,
        n.spend as new_spend,
        o.purchases as old_purch,
        n.purchases as new_purch,
        (n.purchases - o.purchases) as purch_diff,
        o.registrations as old_regs,
        n.registrations as new_regs,
        (n.registrations - o.registrations) as regs_diff
    FROM old_fb o
    INNER JOIN new_fb n ON o.date = n.date
    WHERE o.purchases != n.purchases
       OR o.registrations != n.registrations
    ORDER BY date DESC
    """
    cols, rows = run_query(conn, query)
    if rows:
        print(tabulate(rows, headers=cols, tablefmt='psql', floatfmt='.2f'))
    else:
        print("NO DIFFERENCES FOUND - All purchases and registrations match!")

    # Perfect match days
    print("\n" + "=" * 140)
    print("DAYS WITH PERFECT MATCH (all metrics identical)")
    print("=" * 140)

    query = """
    WITH old_fb AS (
        SELECT
            report_date as date,
            ROUND(SUM(COALESCE(amount_spent, 0))::numeric, 2) as spend,
            SUM(COALESCE(impressions, 0)) as impressions,
            SUM(COALESCE(clicks, 0)) as clicks,
            SUM(COALESCE(purchases, 0)) as purchases,
            SUM(COALESCE(registrations_completed, 0)) as registrations
        FROM raw_facebook.facebook_ad_statistics
        GROUP BY report_date
    ),
    new_fb AS (
        SELECT
            report_date as date,
            ROUND(SUM(COALESCE(amount_spent, 0))::numeric, 2) as spend,
            SUM(COALESCE(impressions, 0)) as impressions,
            SUM(COALESCE(clicks, 0)) as clicks,
            SUM(COALESCE(purchases, 0)) as purchases,
            SUM(COALESCE(registrations_completed, 0)) as registrations
        FROM analytics.stg_facebook_new__ad_statistics
        GROUP BY report_date
    )
    SELECT
        COUNT(*) as perfect_match_days,
        COUNT(*) * 100.0 / (SELECT COUNT(DISTINCT report_date) FROM raw_facebook.facebook_ad_statistics) as pct_perfect
    FROM old_fb o
    INNER JOIN new_fb n ON o.date = n.date
    WHERE o.spend = n.spend
      AND o.impressions = n.impressions
      AND o.clicks = n.clicks
      AND o.purchases = n.purchases
      AND o.registrations = n.registrations
    """
    cols, rows = run_query(conn, query)
    print(tabulate(rows, headers=cols, tablefmt='psql', floatfmt='.2f'))

    conn.close()
    print("\n" + "=" * 140)
    print("COMPARISON COMPLETE")
    print("=" * 140)

if __name__ == '__main__':
    main()
