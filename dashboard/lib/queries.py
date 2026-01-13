"""
Data queries for dashboard.
Currently uses mock data - swap to real DB queries when marts are ready.
"""

import streamlit as st
import pandas as pd
from datetime import date
from . import mock_data


# Generate mock data once and cache
@st.cache_data
def _get_mock_payments() -> pd.DataFrame:
    return mock_data.generate_mock_payments(days=90)


@st.cache_data
def _get_mock_daily() -> pd.DataFrame:
    payments = _get_mock_payments()
    return mock_data.generate_mock_daily_summary(payments)


@st.cache_data(ttl=300)
def get_daily_summary(start_date: date, end_date: date, funnel: str = None) -> pd.DataFrame:
    """
    Get daily payment summary.

    When real DB is ready, replace with:
        query = "SELECT * FROM analytics.mart_stripe_payments_daily WHERE date BETWEEN %s AND %s"
        return pd.read_sql(query, conn, params=[start_date, end_date])
    """
    df = _get_mock_daily()
    df = df[(df["date"] >= start_date) & (df["date"] <= end_date)]
    if funnel and funnel != "All":
        df = df[df["funnel_name"] == funnel]
    return df


@st.cache_data(ttl=300)
def get_overview_metrics(start_date: date, end_date: date) -> dict:
    """Get high-level metrics for the overview cards."""
    df = _get_mock_payments()
    df = df[(df["created_date"] >= start_date) & (df["created_date"] <= end_date)]

    successful = df[df["is_successful"]]
    failed = df[~df["is_successful"]]

    return {
        "total_revenue": successful["amount_usd"].sum(),
        "total_attempts": len(df),
        "successful_count": len(successful),
        "failed_count": len(failed),
        "success_rate": len(successful) / len(df) if len(df) > 0 else 0,
        "failed_revenue": failed["amount_usd"].sum(),
    }


@st.cache_data(ttl=300)
def get_failure_breakdown(start_date: date, end_date: date) -> pd.DataFrame:
    """Get failure counts by category."""
    df = _get_mock_payments()
    df = df[(df["created_date"] >= start_date) & (df["created_date"] <= end_date)]
    failed = df[~df["is_successful"]]

    breakdown = failed.groupby("failure_category").agg(
        count=("charge_id", "count"),
        lost_revenue=("amount_usd", "sum"),
        recovery_action=("recovery_action", "first"),
    ).reset_index().sort_values("count", ascending=False)

    return breakdown


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
    df = _get_mock_payments()

    # Apply filters
    df = df[(df["created_date"] >= start_date) & (df["created_date"] <= end_date)]

    if status == "Successful":
        df = df[df["is_successful"]]
    elif status == "Failed":
        df = df[~df["is_successful"]]

    if funnel != "All":
        df = df[df["funnel_name"] == funnel]

    if card_brand != "All":
        df = df[df["card_brand"] == card_brand]

    if search:
        search_lower = search.lower()
        df = df[
            df["charge_id"].str.lower().str.contains(search_lower, regex=False) |
            df["customer_id"].str.lower().str.contains(search_lower, regex=False) |
            df["profile_id"].str.lower().str.contains(search_lower, regex=False)
        ]

    total_count = len(df)

    # Sort and paginate
    df = df.sort_values("created_at", ascending=False)
    start_idx = (page - 1) * page_size
    df = df.iloc[start_idx:start_idx + page_size]

    return df, total_count


@st.cache_data(ttl=60)
def get_payment_detail(charge_id: str) -> dict | None:
    """Get detailed info for a single payment."""
    df = _get_mock_payments()
    payment = df[df["charge_id"] == charge_id]

    if payment.empty:
        return None

    row = payment.iloc[0]

    # Get retry history (other charges with same payment_intent)
    retries = df[df["payment_intent_id"] == row["payment_intent_id"]].sort_values("created_at")

    return {
        "charge_id": row["charge_id"],
        "payment_intent_id": row["payment_intent_id"],
        "customer_id": row["customer_id"],
        "profile_id": row["profile_id"],
        "amount_usd": row["amount_usd"],
        "currency": row["currency"],
        "status": row["status"],
        "failure_category": row["failure_category"],
        "recovery_action": row["recovery_action"],
        "card_brand": row["card_brand"],
        "card_country": row["card_country"],
        "created_at": row["created_at"],
        "retry_history": retries[["charge_id", "created_at", "status", "failure_category"]].to_dict("records"),
    }


@st.cache_data
def get_filter_options() -> dict:
    """Get unique values for filter dropdowns."""
    df = _get_mock_payments()
    return {
        "funnels": ["All"] + sorted(df["funnel_name"].unique().tolist()),
        "card_brands": ["All"] + sorted(df["card_brand"].unique().tolist()),
        "statuses": ["All", "Successful", "Failed"],
    }
