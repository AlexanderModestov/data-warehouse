"""
Subscriptions Page - New subscription analytics.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, timedelta

from lib import queries

# Auth check
if not st.session_state.get("authenticated", False):
    st.warning("Please login from the main page.")
    st.stop()

st.title("New Subscriptions")

# Date range selector
col1, col2, col3 = st.columns([2, 2, 6])

with col1:
    date_preset = st.selectbox(
        "Date Range",
        ["Last 7 days", "Last 30 days", "Last 90 days", "Custom"],
        index=1,
        key="subs_date_preset",
    )

# Calculate date range
today = date.today()
if date_preset == "Last 7 days":
    start_date = today - timedelta(days=7)
    end_date = today
elif date_preset == "Last 30 days":
    start_date = today - timedelta(days=30)
    end_date = today
elif date_preset == "Last 90 days":
    start_date = today - timedelta(days=90)
    end_date = today
else:
    with col2:
        start_date = st.date_input("Start", today - timedelta(days=30), key="subs_start")
    with col3:
        end_date = st.date_input("End", today, key="subs_end")

# Previous period for comparison
period_length = (end_date - start_date).days
prev_start = start_date - timedelta(days=period_length)
prev_end = start_date - timedelta(days=1)

# Get metrics
with st.spinner("Loading subscription metrics..."):
    current_metrics = queries.get_subscription_metrics(start_date, end_date)
    prev_metrics = queries.get_subscription_metrics(prev_start, prev_end)


# Calculate deltas
def calc_delta(current, previous):
    if previous == 0:
        return 0
    return ((current - previous) / previous) * 100


subs_delta = calc_delta(current_metrics["total_subscriptions"], prev_metrics["total_subscriptions"])
revenue_delta = calc_delta(current_metrics["total_revenue"], prev_metrics["total_revenue"])
users_delta = calc_delta(current_metrics["unique_subscribers"], prev_metrics["unique_subscribers"])
convert_delta = calc_delta(current_metrics["avg_hours_to_convert"], prev_metrics["avg_hours_to_convert"])

st.markdown("---")

# Metric cards
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="New Subscriptions",
        value=f"{current_metrics['total_subscriptions']:,}",
        delta=f"{subs_delta:+.1f}%",
    )

with col2:
    st.metric(
        label="Revenue",
        value=f"${current_metrics['total_revenue']:,.2f}",
        delta=f"{revenue_delta:+.1f}%",
    )

with col3:
    st.metric(
        label="Unique Subscribers",
        value=f"{current_metrics['unique_subscribers']:,}",
        delta=f"{users_delta:+.1f}%",
    )

with col4:
    st.metric(
        label="Avg Hours to Convert",
        value=f"{current_metrics['avg_hours_to_convert']:.1f}h",
        delta=f"{convert_delta:+.1f}%",
        delta_color="inverse",
    )

st.markdown("---")

# Daily subscriptions chart
st.subheader("Daily Subscriptions & Revenue")

daily_data = queries.get_subscriptions_daily(start_date, end_date)

if not daily_data.empty:
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=daily_data["date"],
        y=daily_data["subscriptions"],
        name="Subscriptions",
        yaxis="y",
        marker_color="#2196F3",
    ))

    fig.add_trace(go.Scatter(
        x=daily_data["date"],
        y=daily_data["revenue_usd"],
        name="Revenue ($)",
        yaxis="y2",
        line=dict(color="#4CAF50", width=3),
        mode="lines+markers",
    ))

    fig.update_layout(
        yaxis=dict(title="Subscriptions", side="left"),
        yaxis2=dict(title="Revenue ($)", side="right", overlaying="y"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=400,
        margin=dict(t=30),
    )

    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No subscription data available for the selected date range.")

# Two columns for breakdowns
col1, col2 = st.columns(2)

with col1:
    st.subheader("By Funnel")

    funnel_data = queries.get_subscriptions_by_funnel(start_date, end_date)

    if not funnel_data.empty:
        fig = px.bar(
            funnel_data,
            x="subscriptions",
            y="funnel",
            orientation="h",
            color="revenue_usd",
            color_continuous_scale="Blues",
            labels={"subscriptions": "Subscriptions", "funnel": "Funnel", "revenue_usd": "Revenue"},
        )
        fig.update_layout(height=300, margin=dict(t=20, b=20), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No funnel data available.")

with col2:
    st.subheader("By Country")

    country_data = queries.get_subscriptions_by_country(start_date, end_date)

    if not country_data.empty:
        fig = px.pie(
            country_data,
            values="subscriptions",
            names="country",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig.update_traces(textposition="inside", textinfo="percent+label")
        fig.update_layout(showlegend=False, height=300, margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No country data available.")

# Billing breakdown
st.subheader("Billing Plans")

billing_data = queries.get_subscriptions_by_billing(start_date, end_date)

if not billing_data.empty:
    billing_data["plan"] = billing_data.apply(
        lambda r: f"{r['billing_interval_count']} {r['billing_interval']}"
        if pd.notna(r['billing_interval']) else "Unknown",
        axis=1,
    )
    display_df = billing_data[["plan", "subscriptions", "revenue_usd"]].copy()
    display_df.columns = ["Plan", "Subscriptions", "Revenue"]
    display_df["Revenue"] = display_df["Revenue"].apply(lambda x: f"${x:,.2f}" if x else "$0.00")

    st.dataframe(display_df, use_container_width=True, hide_index=True)
else:
    st.info("No billing data available.")

# Refresh button
st.markdown("---")
if st.button("Refresh Data", key="subs_refresh"):
    st.cache_data.clear()
    st.rerun()
