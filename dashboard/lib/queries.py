"""
Data queries for dashboard.
Uses real database queries against dbt marts.
"""

import streamlit as st
import pandas as pd
from datetime import date
from . import db


@st.cache_data(ttl=300)
def get_daily_summary(start_date: date, end_date: date, funnel: str = None) -> pd.DataFrame:
    """
    Get daily payment summary from mart_stripe_payments_daily.
    Aggregates across dimensions for dashboard display.
    """
    query = """
        SELECT
            date,
            funnel_name,
            SUM(total_attempts) AS total_attempts,
            SUM(successful_payments) AS successful_payments,
            SUM(failed_payments) AS failed_payments,
            SUM(gross_revenue_usd) AS gross_revenue_usd,
            SUM(failed_revenue_usd) AS failed_revenue_usd,
            CASE
                WHEN SUM(total_attempts) > 0
                THEN SUM(successful_payments)::NUMERIC / SUM(total_attempts)
                ELSE NULL
            END AS success_rate
        FROM analytics.mart_stripe_payments_daily
        WHERE date >= %s AND date <= %s
        GROUP BY date, funnel_name
        ORDER BY date ASC
    """
    results = db.execute_query(query, (start_date, end_date))
    df = pd.DataFrame(results)

    if funnel and funnel != "All" and not df.empty:
        df = df[df["funnel_name"] == funnel]

    return df


@st.cache_data(ttl=300)
def get_overview_metrics(start_date: date, end_date: date) -> dict:
    """Get high-level metrics for the overview cards."""
    query = """
        SELECT
            SUM(CASE WHEN is_successful THEN amount_usd ELSE 0 END) AS total_revenue,
            COUNT(*) AS total_attempts,
            SUM(CASE WHEN is_successful THEN 1 ELSE 0 END) AS successful_count,
            SUM(CASE WHEN NOT is_successful THEN 1 ELSE 0 END) AS failed_count,
            SUM(CASE WHEN NOT is_successful THEN amount_usd ELSE 0 END) AS failed_revenue
        FROM analytics.mart_stripe_payments
        WHERE created_date >= %s AND created_date <= %s
    """
    results = db.execute_query(query, (start_date, end_date))

    if results:
        row = results[0]
        total_attempts = row["total_attempts"] or 0
        successful_count = row["successful_count"] or 0
        return {
            "total_revenue": float(row["total_revenue"] or 0),
            "total_attempts": total_attempts,
            "successful_count": successful_count,
            "failed_count": row["failed_count"] or 0,
            "success_rate": successful_count / total_attempts if total_attempts > 0 else 0,
            "failed_revenue": float(row["failed_revenue"] or 0),
        }

    return {
        "total_revenue": 0,
        "total_attempts": 0,
        "successful_count": 0,
        "failed_count": 0,
        "success_rate": 0,
        "failed_revenue": 0,
    }


@st.cache_data(ttl=300)
def get_failure_breakdown(start_date: date, end_date: date) -> pd.DataFrame:
    """Get failure counts by category."""
    query = """
        SELECT
            failure_category,
            COUNT(*) AS count,
            SUM(amount_usd) AS lost_revenue,
            MAX(recovery_action) AS recovery_action
        FROM analytics.mart_stripe_payments
        WHERE created_date >= %s AND created_date <= %s
          AND NOT is_successful
          AND failure_category IS NOT NULL
        GROUP BY failure_category
        ORDER BY count DESC
    """
    results = db.execute_query(query, (start_date, end_date))
    return pd.DataFrame(results)


@st.cache_data(ttl=60)
def get_payments_list(
    start_date: date,
    end_date: date,
    status: str = "All",
    funnel: str = "All",
    card_brand: str = "All",
    search: str = "",
    page: int = 1,
    page_size: int = 50,
) -> tuple[pd.DataFrame, int]:
    """
    Get paginated payment list with filters.
    Returns (dataframe, total_count).
    """
    # Build WHERE conditions
    conditions = ["created_date >= %s", "created_date <= %s"]
    params = [start_date, end_date]

    if status == "Successful":
        conditions.append("is_successful = TRUE")
    elif status == "Failed":
        conditions.append("is_successful = FALSE")

    if funnel != "All":
        conditions.append("funnel_name = %s")
        params.append(funnel)

    if card_brand != "All":
        conditions.append("card_brand = %s")
        params.append(card_brand)

    if search:
        conditions.append("""
            (charge_id ILIKE %s
             OR customer_id ILIKE %s
             OR COALESCE(profile_id, '') ILIKE %s)
        """)
        search_pattern = f"%{search}%"
        params.extend([search_pattern, search_pattern, search_pattern])

    where_clause = " AND ".join(conditions)

    # Get total count
    count_query = f"""
        SELECT COUNT(*) AS total
        FROM analytics.mart_stripe_payments
        WHERE {where_clause}
    """
    count_result = db.execute_query(count_query, tuple(params))
    total_count = count_result[0]["total"] if count_result else 0

    # Get paginated data
    offset = (page - 1) * page_size
    data_query = f"""
        SELECT
            charge_id,
            payment_intent_id,
            customer_id,
            profile_id,
            status,
            is_successful,
            amount_usd,
            currency,
            failure_code,
            failure_category,
            recovery_action,
            attempt_number,
            created_at,
            created_date,
            funnel_name,
            card_brand,
            card_country
        FROM analytics.mart_stripe_payments
        WHERE {where_clause}
        ORDER BY created_at DESC
        LIMIT %s OFFSET %s
    """
    params.extend([page_size, offset])
    results = db.execute_query(data_query, tuple(params))

    return pd.DataFrame(results), total_count


@st.cache_data(ttl=60)
def get_payment_detail(charge_id: str) -> dict | None:
    """Get detailed info for a single payment."""
    query = """
        SELECT
            charge_id,
            payment_intent_id,
            customer_id,
            profile_id,
            amount_usd,
            currency,
            status,
            failure_category,
            recovery_action,
            card_brand,
            card_country,
            created_at
        FROM analytics.mart_stripe_payments
        WHERE charge_id = %s
    """
    results = db.execute_query(query, (charge_id,))

    if not results:
        return None

    row = results[0]

    # Get retry history (other charges with same payment_intent)
    retry_query = """
        SELECT
            charge_id,
            created_at,
            status,
            failure_category
        FROM analytics.mart_stripe_payments
        WHERE payment_intent_id = %s
        ORDER BY created_at ASC
    """
    retry_results = db.execute_query(retry_query, (row["payment_intent_id"],))

    return {
        "charge_id": row["charge_id"],
        "payment_intent_id": row["payment_intent_id"],
        "customer_id": row["customer_id"],
        "profile_id": row["profile_id"],
        "amount_usd": float(row["amount_usd"]) if row["amount_usd"] else 0,
        "currency": row["currency"] or "usd",
        "status": row["status"],
        "failure_category": row["failure_category"],
        "recovery_action": row["recovery_action"],
        "card_brand": row["card_brand"],
        "card_country": row["card_country"],
        "created_at": row["created_at"],
        "retry_history": retry_results,
    }


@st.cache_data(ttl=300)
def get_filter_options() -> dict:
    """Get unique values for filter dropdowns."""
    # Get unique funnel names
    funnel_query = """
        SELECT DISTINCT funnel_name
        FROM analytics.mart_stripe_payments
        WHERE funnel_name IS NOT NULL
        ORDER BY funnel_name
    """
    funnel_results = db.execute_query(funnel_query)
    funnels = ["All"] + [r["funnel_name"] for r in funnel_results]

    # Get unique card brands
    card_query = """
        SELECT DISTINCT card_brand
        FROM analytics.mart_stripe_payments
        WHERE card_brand IS NOT NULL
        ORDER BY card_brand
    """
    card_results = db.execute_query(card_query)
    card_brands = ["All"] + [r["card_brand"] for r in card_results]

    return {
        "funnels": funnels,
        "card_brands": card_brands,
        "statuses": ["All", "Successful", "Failed"],
    }


# ============================================================
# Subscription Queries (mart_new_subscriptions)
# ============================================================

@st.cache_data(ttl=300)
def get_subscription_metrics(start_date: date, end_date: date) -> dict:
    """Get high-level subscription metrics."""
    query = """
        SELECT
            COUNT(*) AS total_subscriptions,
            SUM(revenue_usd) AS total_revenue,
            COUNT(DISTINCT user_profile_id) AS unique_subscribers,
            AVG(hours_to_convert) AS avg_hours_to_convert,
            COUNT(DISTINCT funnel_id) AS active_funnels
        FROM analytics.mart_new_subscriptions
        WHERE subscription_date >= %s AND subscription_date <= %s
    """
    results = db.execute_query(query, (start_date, end_date))

    if results:
        row = results[0]
        return {
            "total_subscriptions": row["total_subscriptions"] or 0,
            "total_revenue": float(row["total_revenue"] or 0),
            "unique_subscribers": row["unique_subscribers"] or 0,
            "avg_hours_to_convert": float(row["avg_hours_to_convert"] or 0),
            "active_funnels": row["active_funnels"] or 0,
        }

    return {
        "total_subscriptions": 0,
        "total_revenue": 0,
        "unique_subscribers": 0,
        "avg_hours_to_convert": 0,
        "active_funnels": 0,
    }


@st.cache_data(ttl=300)
def get_subscriptions_daily(start_date: date, end_date: date) -> pd.DataFrame:
    """Get daily subscription counts and revenue."""
    query = """
        SELECT
            subscription_date AS date,
            COUNT(*) AS subscriptions,
            SUM(revenue_usd) AS revenue_usd,
            COUNT(DISTINCT user_profile_id) AS unique_subscribers
        FROM analytics.mart_new_subscriptions
        WHERE subscription_date >= %s AND subscription_date <= %s
        GROUP BY subscription_date
        ORDER BY subscription_date ASC
    """
    results = db.execute_query(query, (start_date, end_date))
    return pd.DataFrame(results)


@st.cache_data(ttl=300)
def get_subscriptions_by_funnel(start_date: date, end_date: date) -> pd.DataFrame:
    """Get subscription breakdown by funnel."""
    query = """
        SELECT
            COALESCE(funnel_title, 'Unknown') AS funnel,
            COUNT(*) AS subscriptions,
            SUM(revenue_usd) AS revenue_usd,
            AVG(hours_to_convert) AS avg_hours_to_convert
        FROM analytics.mart_new_subscriptions
        WHERE subscription_date >= %s AND subscription_date <= %s
        GROUP BY funnel_title
        ORDER BY subscriptions DESC
    """
    results = db.execute_query(query, (start_date, end_date))
    return pd.DataFrame(results)


@st.cache_data(ttl=300)
def get_subscriptions_by_country(start_date: date, end_date: date) -> pd.DataFrame:
    """Get subscription breakdown by country."""
    query = """
        SELECT
            COALESCE(country, 'Unknown') AS country,
            COUNT(*) AS subscriptions,
            SUM(revenue_usd) AS revenue_usd
        FROM analytics.mart_new_subscriptions
        WHERE subscription_date >= %s AND subscription_date <= %s
        GROUP BY country
        ORDER BY subscriptions DESC
        LIMIT 10
    """
    results = db.execute_query(query, (start_date, end_date))
    return pd.DataFrame(results)


@st.cache_data(ttl=300)
def get_subscriptions_by_billing(start_date: date, end_date: date) -> pd.DataFrame:
    """Get subscription breakdown by billing interval."""
    query = """
        SELECT
            COALESCE(billing_interval, 'Unknown') AS billing_interval,
            billing_interval_count,
            COUNT(*) AS subscriptions,
            SUM(revenue_usd) AS revenue_usd
        FROM analytics.mart_new_subscriptions
        WHERE subscription_date >= %s AND subscription_date <= %s
        GROUP BY billing_interval, billing_interval_count
        ORDER BY subscriptions DESC
    """
    results = db.execute_query(query, (start_date, end_date))
    return pd.DataFrame(results)


# ============================================================
# Funnel Queries (mart_funnel_conversions, mart_funnel_performance)
# ============================================================

@st.cache_data(ttl=300)
def get_funnel_metrics(start_date: date, end_date: date) -> dict:
    """Get high-level funnel metrics."""
    query = """
        SELECT
            COUNT(*) AS total_sessions,
            COUNT(DISTINCT profile_id) AS unique_visitors,
            SUM(converted) AS conversions,
            CASE
                WHEN COUNT(*) > 0
                THEN SUM(converted)::NUMERIC / COUNT(*)
                ELSE 0
            END AS conversion_rate,
            SUM(COALESCE(revenue_usd, 0)) AS total_revenue,
            AVG(time_to_conversion_hours) FILTER (WHERE converted = 1) AS avg_time_to_convert
        FROM analytics.mart_funnel_conversions
        WHERE session_date >= %s AND session_date <= %s
    """
    results = db.execute_query(query, (start_date, end_date))

    if results:
        row = results[0]
        return {
            "total_sessions": row["total_sessions"] or 0,
            "unique_visitors": row["unique_visitors"] or 0,
            "conversions": row["conversions"] or 0,
            "conversion_rate": float(row["conversion_rate"] or 0),
            "total_revenue": float(row["total_revenue"] or 0),
            "avg_time_to_convert": float(row["avg_time_to_convert"] or 0),
        }

    return {
        "total_sessions": 0,
        "unique_visitors": 0,
        "conversions": 0,
        "conversion_rate": 0,
        "total_revenue": 0,
        "avg_time_to_convert": 0,
    }


@st.cache_data(ttl=300)
def get_funnel_performance_daily(start_date: date, end_date: date) -> pd.DataFrame:
    """Get daily funnel performance from mart_funnel_performance."""
    query = """
        SELECT
            date,
            SUM(total_sessions) AS sessions,
            SUM(unique_users) AS unique_users,
            SUM(conversions) AS conversions,
            CASE
                WHEN SUM(total_sessions) > 0
                THEN SUM(conversions)::NUMERIC / SUM(total_sessions)
                ELSE 0
            END AS conversion_rate,
            SUM(revenue_usd) AS revenue_usd
        FROM analytics.mart_funnel_performance
        WHERE date >= %s AND date <= %s
        GROUP BY date
        ORDER BY date ASC
    """
    results = db.execute_query(query, (start_date, end_date))
    return pd.DataFrame(results)


@st.cache_data(ttl=300)
def get_funnel_performance_by_funnel(start_date: date, end_date: date) -> pd.DataFrame:
    """Get performance breakdown by funnel."""
    query = """
        SELECT
            COALESCE(funnel_title, 'Unknown') AS funnel,
            SUM(total_sessions) AS sessions,
            SUM(unique_users) AS unique_users,
            SUM(conversions) AS conversions,
            CASE
                WHEN SUM(total_sessions) > 0
                THEN SUM(conversions)::NUMERIC / SUM(total_sessions)
                ELSE 0
            END AS conversion_rate,
            SUM(revenue_usd) AS revenue_usd,
            AVG(avg_hours_to_convert) AS avg_hours_to_convert
        FROM analytics.mart_funnel_performance
        WHERE date >= %s AND date <= %s
        GROUP BY funnel_title
        ORDER BY sessions DESC
    """
    results = db.execute_query(query, (start_date, end_date))
    return pd.DataFrame(results)


@st.cache_data(ttl=300)
def get_funnel_performance_by_source(start_date: date, end_date: date) -> pd.DataFrame:
    """Get performance breakdown by traffic source."""
    query = """
        SELECT
            traffic_source,
            SUM(total_sessions) AS sessions,
            SUM(unique_users) AS unique_users,
            SUM(conversions) AS conversions,
            CASE
                WHEN SUM(total_sessions) > 0
                THEN SUM(conversions)::NUMERIC / SUM(total_sessions)
                ELSE 0
            END AS conversion_rate,
            SUM(revenue_usd) AS revenue_usd,
            SUM(users_with_amplitude_events) AS users_with_app_engagement
        FROM analytics.mart_funnel_performance
        WHERE date >= %s AND date <= %s
        GROUP BY traffic_source
        ORDER BY sessions DESC
    """
    results = db.execute_query(query, (start_date, end_date))
    return pd.DataFrame(results)


@st.cache_data(ttl=300)
def get_conversion_by_country(start_date: date, end_date: date) -> pd.DataFrame:
    """Get conversion breakdown by country."""
    query = """
        SELECT
            COALESCE(country, 'Unknown') AS country,
            COUNT(*) AS sessions,
            SUM(converted) AS conversions,
            CASE
                WHEN COUNT(*) > 0
                THEN SUM(converted)::NUMERIC / COUNT(*)
                ELSE 0
            END AS conversion_rate,
            SUM(COALESCE(revenue_usd, 0)) AS revenue_usd
        FROM analytics.mart_funnel_conversions
        WHERE session_date >= %s AND session_date <= %s
        GROUP BY country
        ORDER BY sessions DESC
        LIMIT 10
    """
    results = db.execute_query(query, (start_date, end_date))
    return pd.DataFrame(results)
