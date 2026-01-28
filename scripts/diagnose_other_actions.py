"""
Check if registrations and leads have the same duplication issue
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
    print("CHECK REGISTRATION AND LEAD ACTION TYPES FOR DUPLICATION")
    print("=" * 100)

    # 1. Registration action types
    print("\n" + "=" * 100)
    print("1. ALL REGISTRATION-RELATED ACTION TYPES")
    print("=" * 100)

    query = """
    SELECT
        elem->>'action_type' as action_type,
        COUNT(*) as occurrences,
        SUM((elem->>'value')::numeric) as total_value
    FROM raw_facebook_new.adsinsights_default,
         unnest(actions) as elem
    WHERE elem->>'action_type' ILIKE '%registration%'
    GROUP BY elem->>'action_type'
    ORDER BY total_value DESC
    """
    cols, rows = run_query(conn, query)
    print(tabulate(rows, headers=cols, tablefmt='psql'))

    # 2. Lead action types
    print("\n" + "=" * 100)
    print("2. ALL LEAD-RELATED ACTION TYPES")
    print("=" * 100)

    query = """
    SELECT
        elem->>'action_type' as action_type,
        COUNT(*) as occurrences,
        SUM((elem->>'value')::numeric) as total_value
    FROM raw_facebook_new.adsinsights_default,
         unnest(actions) as elem
    WHERE elem->>'action_type' ILIKE '%lead%'
    GROUP BY elem->>'action_type'
    ORDER BY total_value DESC
    """
    cols, rows = run_query(conn, query)
    print(tabulate(rows, headers=cols, tablefmt='psql'))

    # 3. Compare registrations - old vs single action type
    print("\n" + "=" * 100)
    print("3. REGISTRATION COMPARISON: old vs single action type")
    print("=" * 100)

    query = """
    WITH old_fb AS (
        SELECT
            report_date,
            SUM(registrations_completed) as registrations
        FROM raw_facebook.facebook_ad_statistics
        WHERE report_date >= '2026-01-09'
        GROUP BY report_date
    ),
    new_complete_reg AS (
        SELECT
            date_start::date as report_date,
            SUM((elem->>'value')::numeric) as registrations
        FROM raw_facebook_new.adsinsights_default,
             unnest(actions) as elem
        WHERE date_start::date >= '2026-01-09'
          AND elem->>'action_type' = 'complete_registration'
        GROUP BY date_start::date
    ),
    new_omni_reg AS (
        SELECT
            date_start::date as report_date,
            SUM((elem->>'value')::numeric) as registrations
        FROM raw_facebook_new.adsinsights_default,
             unnest(actions) as elem
        WHERE date_start::date >= '2026-01-09'
          AND elem->>'action_type' = 'omni_complete_registration'
        GROUP BY date_start::date
    )
    SELECT
        o.report_date,
        o.registrations as old_regs,
        cr.registrations as new_complete_reg,
        omni.registrations as new_omni_reg
    FROM old_fb o
    LEFT JOIN new_complete_reg cr ON o.report_date = cr.report_date
    LEFT JOIN new_omni_reg omni ON o.report_date = omni.report_date
    ORDER BY o.report_date DESC
    """
    cols, rows = run_query(conn, query)
    print(tabulate(rows, headers=cols, tablefmt='psql'))

    # 4. Compare leads - old vs single action type
    print("\n" + "=" * 100)
    print("4. LEAD COMPARISON: old vs single action type")
    print("=" * 100)

    query = """
    WITH old_fb AS (
        SELECT
            report_date,
            SUM(leads) as leads
        FROM raw_facebook.facebook_ad_statistics
        WHERE report_date >= '2026-01-09'
        GROUP BY report_date
    ),
    new_lead AS (
        SELECT
            date_start::date as report_date,
            SUM((elem->>'value')::numeric) as leads
        FROM raw_facebook_new.adsinsights_default,
             unnest(actions) as elem
        WHERE date_start::date >= '2026-01-09'
          AND elem->>'action_type' = 'lead'
        GROUP BY date_start::date
    ),
    new_offsite_lead AS (
        SELECT
            date_start::date as report_date,
            SUM((elem->>'value')::numeric) as leads
        FROM raw_facebook_new.adsinsights_default,
             unnest(actions) as elem
        WHERE date_start::date >= '2026-01-09'
          AND elem->>'action_type' = 'offsite_conversion.fb_pixel_lead'
        GROUP BY date_start::date
    )
    SELECT
        o.report_date,
        o.leads as old_leads,
        l.leads as new_lead,
        ofl.leads as new_offsite_lead
    FROM old_fb o
    LEFT JOIN new_lead l ON o.report_date = l.report_date
    LEFT JOIN new_offsite_lead ofl ON o.report_date = ofl.report_date
    ORDER BY o.report_date DESC
    """
    cols, rows = run_query(conn, query)
    print(tabulate(rows, headers=cols, tablefmt='psql'))

    conn.close()
    print("\n" + "=" * 100)
    print("DONE")
    print("=" * 100)

if __name__ == '__main__':
    main()
