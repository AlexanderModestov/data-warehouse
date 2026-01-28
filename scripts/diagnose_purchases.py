"""
Diagnose purchase counting issue between raw_facebook and raw_facebook_new
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

    print("=" * 100)
    print("DIAGNOSE PURCHASE COUNTING ISSUE")
    print("=" * 100)

    # 1. Check all purchase-related action types
    print("\n" + "=" * 100)
    print("1. ALL PURCHASE-RELATED ACTION TYPES IN raw_facebook_new")
    print("=" * 100)

    query = """
    SELECT
        elem->>'action_type' as action_type,
        COUNT(*) as occurrences,
        SUM((elem->>'value')::numeric) as total_value
    FROM raw_facebook_new.adsinsights_default,
         unnest(actions) as elem
    WHERE elem->>'action_type' ILIKE '%purchase%'
    GROUP BY elem->>'action_type'
    ORDER BY total_value DESC
    """
    cols, rows = run_query(conn, query)
    print(tabulate(rows, headers=cols, tablefmt='psql'))

    # 2. Check a single day - are the action types duplicating?
    print("\n" + "=" * 100)
    print("2. SINGLE DAY CHECK (Jan 21) - Are action types duplicated per row?")
    print("=" * 100)

    query = """
    SELECT
        ad_id,
        date_start::date as date,
        elem->>'action_type' as action_type,
        (elem->>'value')::numeric as value
    FROM raw_facebook_new.adsinsights_default,
         unnest(actions) as elem
    WHERE date_start::date = '2026-01-21'
      AND elem->>'action_type' ILIKE '%purchase%'
    ORDER BY ad_id, action_type
    LIMIT 30
    """
    cols, rows = run_query(conn, query)
    print(tabulate(rows, headers=cols, tablefmt='psql'))

    # 3. Compare old vs new for specific ads on same day
    print("\n" + "=" * 100)
    print("3. COMPARE OLD vs NEW for Jan 21 - by ad_id")
    print("=" * 100)

    query = """
    WITH old_purchases AS (
        SELECT
            facebook_ad_id as ad_id,
            report_date,
            purchases
        FROM raw_facebook.facebook_ad_statistics
        WHERE report_date = '2026-01-21'
    ),
    new_purchases_detail AS (
        SELECT
            ad_id,
            date_start::date as report_date,
            elem->>'action_type' as action_type,
            (elem->>'value')::numeric as value
        FROM raw_facebook_new.adsinsights_default,
             unnest(actions) as elem
        WHERE date_start::date = '2026-01-21'
          AND elem->>'action_type' ILIKE '%purchase%'
    ),
    new_purchases_sum AS (
        SELECT
            ad_id,
            report_date,
            SUM(value) as total_purchases,
            STRING_AGG(action_type || '=' || value::text, ', ') as breakdown
        FROM new_purchases_detail
        GROUP BY ad_id, report_date
    )
    SELECT
        COALESCE(o.ad_id, n.ad_id) as ad_id,
        o.purchases as old_purchases,
        n.total_purchases as new_purchases,
        n.breakdown as new_breakdown
    FROM old_purchases o
    FULL OUTER JOIN new_purchases_sum n ON o.ad_id = n.ad_id
    WHERE o.purchases > 0 OR n.total_purchases > 0
    ORDER BY COALESCE(o.purchases, 0) DESC
    LIMIT 20
    """
    cols, rows = run_query(conn, query)
    print(tabulate(rows, headers=cols, tablefmt='psql'))

    # 4. Check if the old table uses a specific action type
    print("\n" + "=" * 100)
    print("4. CHECK: Does new 'purchase' (single type) match old?")
    print("=" * 100)

    query = """
    WITH old_fb AS (
        SELECT
            report_date,
            SUM(purchases) as purchases
        FROM raw_facebook.facebook_ad_statistics
        WHERE report_date >= '2026-01-09'
        GROUP BY report_date
    ),
    new_purchase_only AS (
        SELECT
            date_start::date as report_date,
            SUM((elem->>'value')::numeric) as purchases
        FROM raw_facebook_new.adsinsights_default,
             unnest(actions) as elem
        WHERE date_start::date >= '2026-01-09'
          AND elem->>'action_type' = 'purchase'
        GROUP BY date_start::date
    ),
    new_omni_purchase AS (
        SELECT
            date_start::date as report_date,
            SUM((elem->>'value')::numeric) as purchases
        FROM raw_facebook_new.adsinsights_default,
             unnest(actions) as elem
        WHERE date_start::date >= '2026-01-09'
          AND elem->>'action_type' = 'omni_purchase'
        GROUP BY date_start::date
    ),
    new_offsite_purchase AS (
        SELECT
            date_start::date as report_date,
            SUM((elem->>'value')::numeric) as purchases
        FROM raw_facebook_new.adsinsights_default,
             unnest(actions) as elem
        WHERE date_start::date >= '2026-01-09'
          AND elem->>'action_type' = 'offsite_conversion.fb_pixel_purchase'
        GROUP BY date_start::date
    )
    SELECT
        o.report_date,
        o.purchases as old_purchases,
        p.purchases as new_purchase,
        op.purchases as new_omni_purchase,
        ofp.purchases as new_offsite_purchase
    FROM old_fb o
    LEFT JOIN new_purchase_only p ON o.report_date = p.report_date
    LEFT JOIN new_omni_purchase op ON o.report_date = op.report_date
    LEFT JOIN new_offsite_purchase ofp ON o.report_date = ofp.report_date
    ORDER BY o.report_date DESC
    """
    cols, rows = run_query(conn, query)
    print(tabulate(rows, headers=cols, tablefmt='psql'))

    # 5. Check what the old table actually has
    print("\n" + "=" * 100)
    print("5. RAW DATA CHECK - Sample from old table")
    print("=" * 100)

    query = """
    SELECT
        facebook_ad_id,
        report_date,
        purchases,
        amount_spent,
        impressions,
        clicks
    FROM raw_facebook.facebook_ad_statistics
    WHERE report_date = '2026-01-21'
      AND purchases > 0
    ORDER BY purchases DESC
    LIMIT 10
    """
    cols, rows = run_query(conn, query)
    print(tabulate(rows, headers=cols, tablefmt='psql'))

    # 6. Check if staging model is correctly counting
    print("\n" + "=" * 100)
    print("6. STAGING MODEL OUTPUT for same ads")
    print("=" * 100)

    query = """
    SELECT
        facebook_ad_id,
        report_date,
        purchases,
        amount_spent,
        impressions,
        clicks
    FROM analytics.stg_facebook_new__ad_statistics
    WHERE report_date = '2026-01-21'
      AND purchases > 0
    ORDER BY purchases DESC
    LIMIT 10
    """
    cols, rows = run_query(conn, query)
    print(tabulate(rows, headers=cols, tablefmt='psql'))

    conn.close()
    print("\n" + "=" * 100)
    print("DIAGNOSIS COMPLETE")
    print("=" * 100)

if __name__ == '__main__':
    main()
