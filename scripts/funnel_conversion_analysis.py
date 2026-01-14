#!/usr/bin/env python3
"""
Funnel Conversion Analysis - Calculate which funnels convert into paying users.

Analyzes conversion rates by UTM source, campaign, medium, and other attribution dimensions.

Usage:
    python funnel_conversion_analysis.py
    python funnel_conversion_analysis.py --group-by utm_campaign
    python funnel_conversion_analysis.py --output conversions.csv
"""
import os
import sys
import argparse
from pathlib import Path
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load .env from project root
project_root = Path(__file__).parent.parent
env_file = project_root / '.env'

if env_file.exists():
    load_dotenv(env_file)


def get_connection():
    """Create PostgreSQL connection from environment variables."""
    return psycopg2.connect(
        host=os.getenv('PG_ANALYTICS_HOST', 'localhost'),
        port=os.getenv('PG_ANALYTICS_PORT', '5432'),
        database=os.getenv('PG_ANALYTICS_DATABASE', 'warehouse'),
        user=os.getenv('PG_ANALYTICS_USER'),
        password=os.getenv('PG_ANALYTICS_PASSWORD')
    )


def analyze_funnel_conversions(conn, group_by='utm_source', start_date=None, end_date=None):
    """
    Calculate conversion rates by funnel dimension.

    Args:
        conn: PostgreSQL connection
        group_by: Dimension to group by (utm_source, utm_campaign, utm_medium, utm_content, landing_page)
        start_date: Optional start date filter
        end_date: Optional end date filter

    Returns:
        List of dicts with conversion metrics per funnel
    """
    valid_dimensions = ['utm_source', 'utm_campaign', 'utm_medium', 'utm_content', 'utm_term', 'landing_page']
    if group_by not in valid_dimensions:
        raise ValueError(f"group_by must be one of: {valid_dimensions}")

    query = f"""
    WITH funnel_users AS (
        -- All users who entered through web (FunnelFox)
        SELECT
            s.profile_id,
            s.{group_by} AS funnel_dimension,
            s.created_at AS session_created_at,
            s.email
        FROM funnelfox_raw.sessions s
        WHERE s.profile_id IS NOT NULL
          AND s.{group_by} IS NOT NULL
          AND s.{group_by} != ''
        {{date_filter}}
    ),

    app_engaged_users AS (
        -- Users who engaged with the app (Amplitude events)
        SELECT DISTINCT e.user_id AS profile_id
        FROM tap_airbyte.events e
        WHERE e.user_id IS NOT NULL
    ),

    paying_users AS (
        -- Users who made a successful payment
        SELECT
            sub.profile_id,
            SUM(c.amount / 100.0) AS total_revenue,
            COUNT(c.id) AS num_charges,
            MIN(c.created) AS first_payment_at
        FROM tap_stripe.charges c
        INNER JOIN funnelfox_raw.subscriptions sub
            ON c.id = sub.psp_id
        WHERE c.status = 'succeeded'
        GROUP BY sub.profile_id
    )

    SELECT
        fu.funnel_dimension,

        -- Volume metrics
        COUNT(DISTINCT fu.profile_id) AS total_users,
        COUNT(DISTINCT ae.profile_id) AS app_engaged_users,
        COUNT(DISTINCT pu.profile_id) AS paying_users,

        -- Conversion rates
        ROUND(
            100.0 * COUNT(DISTINCT ae.profile_id) / NULLIF(COUNT(DISTINCT fu.profile_id), 0),
            2
        ) AS app_engagement_rate,

        ROUND(
            100.0 * COUNT(DISTINCT pu.profile_id) / NULLIF(COUNT(DISTINCT fu.profile_id), 0),
            2
        ) AS conversion_rate,

        ROUND(
            100.0 * COUNT(DISTINCT pu.profile_id) / NULLIF(COUNT(DISTINCT ae.profile_id), 0),
            2
        ) AS engaged_to_paid_rate,

        -- Revenue metrics
        COALESCE(SUM(pu.total_revenue), 0) AS total_revenue,
        ROUND(
            COALESCE(SUM(pu.total_revenue), 0) / NULLIF(COUNT(DISTINCT pu.profile_id), 0),
            2
        ) AS avg_revenue_per_paying_user,
        ROUND(
            COALESCE(SUM(pu.total_revenue), 0) / NULLIF(COUNT(DISTINCT fu.profile_id), 0),
            2
        ) AS revenue_per_user

    FROM funnel_users fu
    LEFT JOIN app_engaged_users ae ON fu.profile_id = ae.profile_id
    LEFT JOIN paying_users pu ON fu.profile_id = pu.profile_id

    GROUP BY fu.funnel_dimension
    HAVING COUNT(DISTINCT fu.profile_id) >= 10  -- Filter low-volume funnels

    ORDER BY COUNT(DISTINCT pu.profile_id) DESC, conversion_rate DESC
    """

    # Build date filter
    date_filter = ""
    params = {}

    if start_date:
        date_filter += " AND s.created_at >= %(start_date)s"
        params['start_date'] = start_date

    if end_date:
        date_filter += " AND s.created_at < %(end_date)s"
        params['end_date'] = end_date

    formatted_query = query.format(date_filter=date_filter)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(formatted_query, params)
        return cur.fetchall()


def analyze_full_funnel(conn, start_date=None, end_date=None):
    """
    Analyze the complete funnel with step-by-step conversion rates.

    Returns overall funnel metrics from session -> app engagement -> payment.
    """
    query = """
    WITH funnel_steps AS (
        SELECT
            'Step 1: Web Session' AS step,
            1 AS step_order,
            COUNT(DISTINCT s.profile_id) AS users
        FROM funnelfox_raw.sessions s
        WHERE s.profile_id IS NOT NULL
        {date_filter_sessions}

        UNION ALL

        SELECT
            'Step 2: App Engagement' AS step,
            2 AS step_order,
            COUNT(DISTINCT e.user_id) AS users
        FROM tap_airbyte.events e
        WHERE e.user_id IN (
            SELECT profile_id FROM funnelfox_raw.sessions WHERE profile_id IS NOT NULL
        )
        {date_filter_events}

        UNION ALL

        SELECT
            'Step 3: Payment' AS step,
            3 AS step_order,
            COUNT(DISTINCT sub.profile_id) AS users
        FROM tap_stripe.charges c
        INNER JOIN funnelfox_raw.subscriptions sub ON c.id = sub.psp_id
        WHERE c.status = 'succeeded'
          AND sub.profile_id IN (
              SELECT profile_id FROM funnelfox_raw.sessions WHERE profile_id IS NOT NULL
          )
        {date_filter_charges}
    )

    SELECT
        step,
        users,
        LAG(users) OVER (ORDER BY step_order) AS previous_step_users,
        ROUND(
            100.0 * users / NULLIF(FIRST_VALUE(users) OVER (ORDER BY step_order), 0),
            2
        ) AS pct_of_total,
        ROUND(
            100.0 * users / NULLIF(LAG(users) OVER (ORDER BY step_order), 0),
            2
        ) AS step_conversion_rate
    FROM funnel_steps
    ORDER BY step_order
    """

    date_filter_sessions = ""
    date_filter_events = ""
    date_filter_charges = ""
    params = {}

    if start_date:
        date_filter_sessions = "AND s.created_at >= %(start_date)s"
        date_filter_events = "AND e.event_time >= %(start_date)s"
        date_filter_charges = "AND c.created >= %(start_date)s"
        params['start_date'] = start_date

    if end_date:
        date_filter_sessions += " AND s.created_at < %(end_date)s"
        date_filter_events += " AND e.event_time < %(end_date)s"
        date_filter_charges += " AND c.created < %(end_date)s"
        params['end_date'] = end_date

    formatted_query = query.format(
        date_filter_sessions=date_filter_sessions,
        date_filter_events=date_filter_events,
        date_filter_charges=date_filter_charges
    )

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(formatted_query, params)
        return cur.fetchall()


def print_funnel_table(data, group_by):
    """Print conversion data as formatted table."""
    if not data:
        print("No data found")
        return

    # Header
    print(f"\n{'='*100}")
    print(f"FUNNEL CONVERSION ANALYSIS BY {group_by.upper()}")
    print(f"{'='*100}\n")

    # Column headers
    print(f"{'Funnel':<30} {'Users':>8} {'Engaged':>8} {'Paying':>8} {'Conv %':>8} {'Revenue':>12} {'RPU':>10}")
    print("-" * 100)

    for row in data:
        funnel = str(row['funnel_dimension'])[:28]
        print(
            f"{funnel:<30} "
            f"{row['total_users']:>8} "
            f"{row['app_engaged_users']:>8} "
            f"{row['paying_users']:>8} "
            f"{row['conversion_rate'] or 0:>7.1f}% "
            f"${row['total_revenue']:>10,.0f} "
            f"${row['revenue_per_user'] or 0:>9.2f}"
        )

    print("-" * 100)

    # Summary
    total_users = sum(r['total_users'] for r in data)
    total_paying = sum(r['paying_users'] for r in data)
    total_revenue = sum(r['total_revenue'] for r in data)

    print(f"{'TOTAL':<30} {total_users:>8} {'-':>8} {total_paying:>8} "
          f"{100*total_paying/total_users if total_users else 0:>7.1f}% "
          f"${total_revenue:>10,.0f} ${total_revenue/total_users if total_users else 0:>9.2f}")


def print_full_funnel(data):
    """Print full funnel analysis."""
    if not data:
        print("No data found")
        return

    print(f"\n{'='*70}")
    print("FULL FUNNEL OVERVIEW")
    print(f"{'='*70}\n")

    print(f"{'Step':<25} {'Users':>10} {'% of Total':>12} {'Step Conv':>12}")
    print("-" * 70)

    for row in data:
        step_conv = f"{row['step_conversion_rate']:.1f}%" if row['step_conversion_rate'] else "-"
        print(
            f"{row['step']:<25} "
            f"{row['users']:>10,} "
            f"{row['pct_of_total']:>11.1f}% "
            f"{step_conv:>12}"
        )


def save_to_csv(data, output_path):
    """Save data to CSV file."""
    if not data:
        print("No data to save")
        return

    import csv

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

    print(f"\nSaved {len(data)} rows to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Analyze funnel conversion rates into paying users'
    )
    parser.add_argument(
        '--group-by', '-g',
        choices=['utm_source', 'utm_campaign', 'utm_medium', 'utm_content', 'utm_term', 'landing_page'],
        default='utm_source',
        help='Dimension to group by (default: utm_source)'
    )
    parser.add_argument(
        '--output', '-o',
        help='Output CSV file path'
    )
    parser.add_argument(
        '--start-date',
        help='Start date (YYYY-MM-DD, UTC)'
    )
    parser.add_argument(
        '--end-date',
        help='End date (YYYY-MM-DD, UTC)'
    )
    parser.add_argument(
        '--full-funnel',
        action='store_true',
        help='Show full funnel overview instead of by-dimension analysis'
    )

    args = parser.parse_args()

    # Parse dates
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d') if args.start_date else None
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d') if args.end_date else None

    try:
        conn = get_connection()
        print("Connected to PostgreSQL")

        if args.full_funnel:
            data = analyze_full_funnel(conn, start_date, end_date)
            print_full_funnel(data)
        else:
            data = analyze_funnel_conversions(conn, args.group_by, start_date, end_date)
            print_funnel_table(data, args.group_by)

        if args.output:
            save_to_csv(data, args.output)

        conn.close()

    except psycopg2.Error as e:
        print(f"Database error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
