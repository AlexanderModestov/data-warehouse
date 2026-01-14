#!/usr/bin/env python3
"""
Full-funnel attribution data extraction from PostgreSQL.

Extracts data linking Meta Ads -> FunnelFox (Web) -> Stripe (Payment) -> Amplitude (App).

Usage:
    python extract_funnel_data.py
    python extract_funnel_data.py --output funnel_data.csv
    python extract_funnel_data.py --start-date 2024-01-01 --end-date 2024-12-31
"""
import os
import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load .env from project root
project_root = Path(__file__).parent.parent
env_file = project_root / '.env'

if env_file.exists():
    load_dotenv(env_file)
    print(f"Loaded environment from {env_file}")
else:
    print(f"Warning: {env_file} not found")


def get_connection():
    """Create PostgreSQL connection from environment variables."""
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        database=os.getenv('POSTGRES_DATABASE', 'warehouse'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )


def extract_funnel_data(conn, start_date=None, end_date=None):
    """
    Extract full-funnel attribution data.

    Joins:
    - funnelfox_raw.sessions (web entry, profile_id is Master ID)
    - tap_airbyte.events (Amplitude app events, user_id = profile_id)
    - tap_stripe.charges (payments, charges.id = subscriptions.psp_id)

    Args:
        conn: PostgreSQL connection
        start_date: Optional start date filter (UTC)
        end_date: Optional end date filter (UTC)

    Returns:
        List of dicts with funnel data
    """
    query = """
    WITH sessions_with_ads AS (
        -- Web sessions with UTM/ad attribution
        SELECT
            s.profile_id,
            s.email,
            s.device_id,
            s.created_at AS session_created_at,
            s.utm_source,
            s.utm_medium,
            s.utm_campaign,
            s.utm_content,
            s.utm_term,
            s.fbclid,
            s.landing_page
        FROM funnelfox_raw.sessions s
        WHERE s.profile_id IS NOT NULL
        {date_filter_sessions}
    ),

    amplitude_events AS (
        -- App events from Amplitude
        SELECT
            e.user_id AS profile_id,
            e.event_type,
            e.event_time,
            e.device_id AS amplitude_device_id,
            e.session_id AS amplitude_session_id,
            e.event_properties
        FROM tap_airbyte.events e
        WHERE e.user_id IS NOT NULL
        {date_filter_events}
    ),

    stripe_payments AS (
        -- Successful payments from Stripe
        SELECT
            sub.profile_id,
            c.id AS charge_id,
            c.amount / 100.0 AS revenue_usd,  -- Stripe stores in cents
            c.currency,
            c.status AS charge_status,
            c.created AS charge_created_at,
            c.failure_code,
            c.failure_message
        FROM tap_stripe.charges c
        INNER JOIN funnelfox_raw.subscriptions sub
            ON c.id = sub.psp_id
        WHERE c.status = 'succeeded'
        {date_filter_charges}
    )

    SELECT
        s.profile_id,
        s.email,
        s.session_created_at,

        -- Attribution fields (Meta Ads)
        s.utm_source,
        s.utm_medium,
        s.utm_campaign,
        s.utm_content,
        s.utm_term,
        s.fbclid,
        s.landing_page,

        -- Amplitude engagement
        COUNT(DISTINCT ae.event_type) AS unique_event_types,
        COUNT(ae.event_type) AS total_events,
        MIN(ae.event_time) AS first_app_event,
        MAX(ae.event_time) AS last_app_event,

        -- Revenue
        COALESCE(SUM(sp.revenue_usd), 0) AS total_revenue_usd,
        COUNT(sp.charge_id) AS successful_charges,
        MIN(sp.charge_created_at) AS first_charge_at,

        -- Conversion flags
        CASE WHEN COUNT(ae.event_type) > 0 THEN TRUE ELSE FALSE END AS has_app_engagement,
        CASE WHEN COUNT(sp.charge_id) > 0 THEN TRUE ELSE FALSE END AS has_converted

    FROM sessions_with_ads s
    LEFT JOIN amplitude_events ae ON s.profile_id = ae.profile_id
    LEFT JOIN stripe_payments sp ON s.profile_id = sp.profile_id

    GROUP BY
        s.profile_id,
        s.email,
        s.session_created_at,
        s.utm_source,
        s.utm_medium,
        s.utm_campaign,
        s.utm_content,
        s.utm_term,
        s.fbclid,
        s.landing_page

    ORDER BY s.session_created_at DESC
    """

    # Build date filters
    date_filter_sessions = ""
    date_filter_events = ""
    date_filter_charges = ""
    params = {}

    if start_date:
        date_filter_sessions += " AND s.created_at >= %(start_date)s"
        date_filter_events += " AND e.event_time >= %(start_date)s"
        date_filter_charges += " AND c.created >= %(start_date)s"
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


def extract_amplitude_events(conn, start_date=None, end_date=None, event_types=None):
    """
    Extract Amplitude events with user attribution.

    Args:
        conn: PostgreSQL connection
        start_date: Optional start date filter (UTC)
        end_date: Optional end date filter (UTC)
        event_types: Optional list of event types to filter

    Returns:
        List of dicts with event data
    """
    query = """
    SELECT
        e.user_id AS profile_id,
        e.event_type,
        e.event_time,
        e.device_id,
        e.session_id,
        e.platform,
        e.os_name,
        e.os_version,
        e.device_type,
        e.country,
        e.city,
        e.event_properties,
        e.user_properties
    FROM tap_airbyte.events e
    WHERE 1=1
    """

    params = {}

    if start_date:
        query += " AND e.event_time >= %(start_date)s"
        params['start_date'] = start_date

    if end_date:
        query += " AND e.event_time < %(end_date)s"
        params['end_date'] = end_date

    if event_types:
        query += " AND e.event_type = ANY(%(event_types)s)"
        params['event_types'] = event_types

    query += " ORDER BY e.event_time DESC"

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params)
        return cur.fetchall()


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

    print(f"Saved {len(data)} rows to {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Extract full-funnel attribution data from PostgreSQL'
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
        '--type',
        choices=['funnel', 'amplitude'],
        default='funnel',
        help='Type of data to extract (default: funnel)'
    )

    args = parser.parse_args()

    # Parse dates
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d') if args.start_date else None
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d') if args.end_date else None

    try:
        conn = get_connection()
        print("Connected to PostgreSQL")

        if args.type == 'funnel':
            print("Extracting full-funnel attribution data...")
            data = extract_funnel_data(conn, start_date, end_date)
        else:
            print("Extracting Amplitude events...")
            data = extract_amplitude_events(conn, start_date, end_date)

        print(f"Retrieved {len(data)} rows")

        if args.output:
            save_to_csv(data, args.output)
        else:
            # Print first few rows as preview
            for row in data[:5]:
                print(row)
            if len(data) > 5:
                print(f"... and {len(data) - 5} more rows")

        conn.close()

    except psycopg2.Error as e:
        print(f"Database error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
