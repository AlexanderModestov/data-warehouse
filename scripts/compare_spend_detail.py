"""
Deep dive into missing spend on specific dates.
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dashboard"))

from lib.db import execute_query


def main():
    print("=" * 70)
    print("DETAILED SPEND ANALYSIS FOR DATES WITH DIFFERENCES")
    print("=" * 70)

    # 1. Ads in raw but not in mart for Jan 21-22
    print("\n1. ADS IN RAW BUT NOT IN MART (Jan 21-22):")
    print("-" * 70)

    query = """
        WITH raw_ads AS (
            SELECT DISTINCT
                report_date,
                facebook_ad_id,
                facebook_campaign_id,
                campaign_name,
                amount_spent
            FROM raw_facebook.facebook_ad_statistics
            WHERE report_date IN ('2026-01-21', '2026-01-22')
        ),
        mart_ads AS (
            SELECT DISTINCT
                date,
                facebook_ad_id
            FROM analytics.mart_marketing_performance
            WHERE date IN ('2026-01-21', '2026-01-22')
        )
        SELECT
            r.report_date,
            r.facebook_ad_id,
            r.facebook_campaign_id,
            r.campaign_name,
            r.amount_spent
        FROM raw_ads r
        LEFT JOIN mart_ads m
            ON r.report_date = m.date
            AND r.facebook_ad_id = m.facebook_ad_id
        WHERE m.facebook_ad_id IS NULL
        ORDER BY r.report_date, r.amount_spent DESC
    """
    result = execute_query(query)

    total_missing = 0
    print(f"  {'Date':<12} {'Ad ID':<22} {'Spend':>12} Campaign")
    print(f"  {'-'*12} {'-'*22} {'-'*12} {'-'*30}")
    for row in result:
        spend = row['amount_spent'] or 0
        total_missing += spend
        camp_name = (row['campaign_name'] or 'NULL')[:40]
        print(f"  {str(row['report_date']):<12} {row['facebook_ad_id']:<22} ${spend:>11,.2f} {camp_name}")

    print(f"\n  TOTAL MISSING SPEND: ${total_missing:,.2f}")
    print(f"  MISSING AD COUNT: {len(result)}")

    # 2. Check if these campaigns exist in campaign metadata
    print("\n2. CHECK IF MISSING CAMPAIGNS EXIST IN METADATA:")
    print("-" * 70)

    query2 = """
        WITH missing_campaigns AS (
            SELECT DISTINCT r.facebook_campaign_id
            FROM raw_facebook.facebook_ad_statistics r
            LEFT JOIN analytics.mart_marketing_performance m
                ON r.report_date = m.date
                AND r.facebook_ad_id = m.facebook_ad_id
            WHERE r.report_date IN ('2026-01-21', '2026-01-22')
              AND m.facebook_ad_id IS NULL
        )
        SELECT
            mc.facebook_campaign_id,
            c.campaign_name,
            c.status
        FROM missing_campaigns mc
        LEFT JOIN raw_facebook.facebook_campaigns c
            ON mc.facebook_campaign_id = c.facebook_campaign_id
    """
    result2 = execute_query(query2)

    for row in result2:
        status = row['status'] or 'NO METADATA'
        name = row['campaign_name'] or 'NO METADATA'
        print(f"  {row['facebook_campaign_id']}: {status} - {name[:50]}")

    # 3. Check the mart SQL model logic - what could filter these out
    print("\n3. CHECK MART MATERIALIZATION DATE:")
    print("-" * 70)

    query3 = """
        SELECT
            MAX(date) AS latest_date_in_mart,
            MIN(date) AS earliest_date_in_mart,
            COUNT(DISTINCT date) AS total_dates
        FROM analytics.mart_marketing_performance
    """
    result3 = execute_query(query3)[0]
    print(f"  Mart date range: {result3['earliest_date_in_mart']} to {result3['latest_date_in_mart']}")
    print(f"  Total dates in mart: {result3['total_dates']}")

    query4 = """
        SELECT
            MAX(report_date) AS latest_date_in_raw,
            MIN(report_date) AS earliest_date_in_raw,
            COUNT(DISTINCT report_date) AS total_dates
        FROM raw_facebook.facebook_ad_statistics
    """
    result4 = execute_query(query4)[0]
    print(f"  Raw date range:  {result4['earliest_date_in_raw']} to {result4['latest_date_in_raw']}")
    print(f"  Total dates in raw: {result4['total_dates']}")

    # 4. Check ad metadata table
    print("\n4. CHECK IF MISSING ADS EXIST IN AD METADATA TABLE:")
    print("-" * 70)

    query5 = """
        WITH missing_ads AS (
            SELECT DISTINCT r.facebook_ad_id
            FROM raw_facebook.facebook_ad_statistics r
            LEFT JOIN analytics.mart_marketing_performance m
                ON r.report_date = m.date
                AND r.facebook_ad_id = m.facebook_ad_id
            WHERE r.report_date IN ('2026-01-21', '2026-01-22')
              AND m.facebook_ad_id IS NULL
        )
        SELECT
            ma.facebook_ad_id,
            a.ad_name,
            a.status,
            CASE WHEN a.facebook_ad_id IS NULL THEN 'NOT IN METADATA' ELSE 'EXISTS' END AS in_metadata
        FROM missing_ads ma
        LEFT JOIN raw_facebook.facebook_ads a
            ON ma.facebook_ad_id = a.facebook_ad_id
    """
    result5 = execute_query(query5)

    in_metadata = sum(1 for r in result5 if r['in_metadata'] == 'EXISTS')
    not_in_metadata = sum(1 for r in result5 if r['in_metadata'] == 'NOT IN METADATA')
    print(f"  Ads in metadata:     {in_metadata}")
    print(f"  Ads NOT in metadata: {not_in_metadata}")

    if not_in_metadata > 0:
        print("\n  Missing from metadata:")
        for row in result5:
            if row['in_metadata'] == 'NOT IN METADATA':
                print(f"    {row['facebook_ad_id']}")

    print("\n" + "=" * 70)
    print("ANALYSIS COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
