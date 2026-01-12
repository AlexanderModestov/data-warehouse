"""
Overview Page - High-level payment metrics and charts.
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

st.title("📊 Payments Overview")

# Date range selector
col1, col2, col3 = st.columns([2, 2, 6])

with col1:
    date_preset = st.selectbox(
        "Date Range",
        ["Last 7 days", "Last 30 days", "Last 90 days", "Custom"],
        index=0,
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
        start_date = st.date_input("Start", today - timedelta(days=30))
    with col3:
        end_date = st.date_input("End", today)

# Previous period for comparison
period_length = (end_date - start_date).days
prev_start = start_date - timedelta(days=period_length)
prev_end = start_date - timedelta(days=1)

# Get metrics
with st.spinner("Loading metrics..."):
    current_metrics = queries.get_overview_metrics(start_date, end_date)
    prev_metrics = queries.get_overview_metrics(prev_start, prev_end)

# Calculate deltas
def calc_delta(current, previous):
    if previous == 0:
        return 0
    return ((current - previous) / previous) * 100

revenue_delta = calc_delta(current_metrics["total_revenue"], prev_metrics["total_revenue"])
rate_delta = calc_delta(current_metrics["success_rate"], prev_metrics["success_rate"])
failed_delta = calc_delta(current_metrics["failed_count"], prev_metrics["failed_count"])
attempts_delta = calc_delta(current_metrics["total_attempts"], prev_metrics["total_attempts"])

st.markdown("---")

# Metric cards
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="Total Revenue",
        value=f"${current_metrics['total_revenue']:,.2f}",
        delta=f"{revenue_delta:+.1f}%",
    )

with col2:
    st.metric(
        label="Success Rate",
        value=f"{current_metrics['success_rate']*100:.1f}%",
        delta=f"{rate_delta:+.1f}%",
    )

with col3:
    st.metric(
        label="Failed Payments",
        value=f"{current_metrics['failed_count']:,}",
        delta=f"{failed_delta:+.1f}%",
        delta_color="inverse",
    )

with col4:
    st.metric(
        label="Total Attempts",
        value=f"{current_metrics['total_attempts']:,}",
        delta=f"{attempts_delta:+.1f}%",
    )

st.markdown("---")

# Daily trend chart
st.subheader("Daily Revenue & Success Rate")

daily_data = queries.get_daily_summary(start_date, end_date)

if not daily_data.empty:
    # Aggregate across funnels for the chart
    daily_agg = daily_data.groupby("date").agg({
        "gross_revenue_usd": "sum",
        "successful_payments": "sum",
        "total_attempts": "sum",
    }).reset_index()
    daily_agg["success_rate"] = daily_agg["successful_payments"] / daily_agg["total_attempts"]

    # Create dual-axis chart
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=daily_agg["date"],
        y=daily_agg["gross_revenue_usd"],
        name="Revenue ($)",
        yaxis="y",
        marker_color="#4CAF50",
    ))

    fig.add_trace(go.Scatter(
        x=daily_agg["date"],
        y=daily_agg["success_rate"] * 100,
        name="Success Rate (%)",
        yaxis="y2",
        line=dict(color="#FF6B6B", width=3),
        mode="lines+markers",
    ))

    # Dynamic y-axis range for success rate (min - 5%, capped at 0)
    min_rate = max(0, (daily_agg["success_rate"].min() * 100) - 5)
    fig.update_layout(
        yaxis=dict(title="Revenue ($)", side="left"),
        yaxis2=dict(title="Success Rate (%)", side="right", overlaying="y", range=[min_rate, 100]),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=400,
        margin=dict(t=30),
    )

    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No data available for the selected date range.")

# Two columns for failure breakdown
col1, col2 = st.columns(2)

with col1:
    st.subheader("Failure Breakdown")

    failure_data = queries.get_failure_breakdown(start_date, end_date)

    if not failure_data.empty:
        fig = px.pie(
            failure_data,
            values="count",
            names="failure_category",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig.update_traces(textposition="inside", textinfo="percent+label")
        fig.update_layout(showlegend=False, height=350, margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No failures in this period! 🎉")

with col2:
    st.subheader("Top Failure Reasons")

    if not failure_data.empty:
        display_df = failure_data[["failure_category", "count", "lost_revenue", "recovery_action"]].copy()
        display_df.columns = ["Category", "Count", "Lost Revenue", "Action"]
        display_df["Lost Revenue"] = display_df["Lost Revenue"].apply(lambda x: f"${x:,.2f}")

        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No failures to display.")

# Refresh button
st.markdown("---")
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()
