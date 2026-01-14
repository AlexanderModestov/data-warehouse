"""
Funnels Page - Conversion analytics and traffic source performance.
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

st.title("Funnel Analytics")

# Date range selector
col1, col2, col3 = st.columns([2, 2, 6])

with col1:
    date_preset = st.selectbox(
        "Date Range",
        ["Last 7 days", "Last 30 days", "Last 90 days", "Custom"],
        index=1,
        key="funnel_date_preset",
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
        start_date = st.date_input("Start", today - timedelta(days=30), key="funnel_start")
    with col3:
        end_date = st.date_input("End", today, key="funnel_end")

# Previous period for comparison
period_length = (end_date - start_date).days
prev_start = start_date - timedelta(days=period_length)
prev_end = start_date - timedelta(days=1)

# Get metrics
with st.spinner("Loading funnel metrics..."):
    current_metrics = queries.get_funnel_metrics(start_date, end_date)
    prev_metrics = queries.get_funnel_metrics(prev_start, prev_end)


# Calculate deltas
def calc_delta(current, previous):
    if previous == 0:
        return 0
    return ((current - previous) / previous) * 100


sessions_delta = calc_delta(current_metrics["total_sessions"], prev_metrics["total_sessions"])
visitors_delta = calc_delta(current_metrics["unique_visitors"], prev_metrics["unique_visitors"])
conversions_delta = calc_delta(current_metrics["conversions"], prev_metrics["conversions"])
rate_delta = calc_delta(current_metrics["conversion_rate"], prev_metrics["conversion_rate"])
revenue_delta = calc_delta(current_metrics["total_revenue"], prev_metrics["total_revenue"])

st.markdown("---")

# Metric cards - row 1
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric(
        label="Sessions",
        value=f"{current_metrics['total_sessions']:,}",
        delta=f"{sessions_delta:+.1f}%",
    )

with col2:
    st.metric(
        label="Unique Visitors",
        value=f"{current_metrics['unique_visitors']:,}",
        delta=f"{visitors_delta:+.1f}%",
    )

with col3:
    st.metric(
        label="Conversions",
        value=f"{current_metrics['conversions']:,}",
        delta=f"{conversions_delta:+.1f}%",
    )

with col4:
    st.metric(
        label="Conversion Rate",
        value=f"{current_metrics['conversion_rate']*100:.2f}%",
        delta=f"{rate_delta:+.1f}%",
    )

with col5:
    st.metric(
        label="Revenue",
        value=f"${current_metrics['total_revenue']:,.2f}",
        delta=f"{revenue_delta:+.1f}%",
    )

st.markdown("---")

# Daily performance chart
st.subheader("Daily Sessions & Conversion Rate")

daily_data = queries.get_funnel_performance_daily(start_date, end_date)

if not daily_data.empty:
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=daily_data["date"],
        y=daily_data["sessions"],
        name="Sessions",
        yaxis="y",
        marker_color="#9C27B0",
    ))

    fig.add_trace(go.Scatter(
        x=daily_data["date"],
        y=daily_data["conversion_rate"] * 100,
        name="Conversion Rate (%)",
        yaxis="y2",
        line=dict(color="#FF9800", width=3),
        mode="lines+markers",
    ))

    # Dynamic y-axis range for conversion rate
    min_rate = max(0, (daily_data["conversion_rate"].min() * 100) - 1)
    max_rate = min(100, (daily_data["conversion_rate"].max() * 100) + 1)

    fig.update_layout(
        yaxis=dict(title="Sessions", side="left"),
        yaxis2=dict(title="Conversion Rate (%)", side="right", overlaying="y", range=[min_rate, max_rate]),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=400,
        margin=dict(t=30),
    )

    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No funnel data available for the selected date range.")

# Two columns for breakdowns
col1, col2 = st.columns(2)

with col1:
    st.subheader("Performance by Funnel")

    funnel_data = queries.get_funnel_performance_by_funnel(start_date, end_date)

    if not funnel_data.empty:
        display_df = funnel_data[["funnel", "sessions", "conversions", "conversion_rate", "revenue_usd"]].copy()
        display_df.columns = ["Funnel", "Sessions", "Conversions", "Conv. Rate", "Revenue"]
        display_df["Conv. Rate"] = display_df["Conv. Rate"].apply(lambda x: f"{x*100:.2f}%")
        display_df["Revenue"] = display_df["Revenue"].apply(lambda x: f"${x:,.2f}" if x else "$0.00")

        st.dataframe(display_df, use_container_width=True, hide_index=True)
    else:
        st.info("No funnel data available.")

with col2:
    st.subheader("Performance by Traffic Source")

    source_data = queries.get_funnel_performance_by_source(start_date, end_date)

    if not source_data.empty:
        # Bar chart for traffic sources
        fig = px.bar(
            source_data,
            x="traffic_source",
            y="sessions",
            color="conversion_rate",
            color_continuous_scale="RdYlGn",
            labels={"sessions": "Sessions", "traffic_source": "Source", "conversion_rate": "Conv. Rate"},
        )
        fig.update_layout(height=300, margin=dict(t=20, b=20))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No traffic source data available.")

# Country breakdown
st.subheader("Conversions by Country")

country_data = queries.get_conversion_by_country(start_date, end_date)

if not country_data.empty:
    col1, col2 = st.columns(2)

    with col1:
        fig = px.pie(
            country_data,
            values="sessions",
            names="country",
            title="Sessions by Country",
            color_discrete_sequence=px.colors.qualitative.Pastel,
        )
        fig.update_traces(textposition="inside", textinfo="percent+label")
        fig.update_layout(showlegend=False, height=350, margin=dict(t=40, b=20))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        display_df = country_data[["country", "sessions", "conversions", "conversion_rate", "revenue_usd"]].copy()
        display_df.columns = ["Country", "Sessions", "Conversions", "Conv. Rate", "Revenue"]
        display_df["Conv. Rate"] = display_df["Conv. Rate"].apply(lambda x: f"{x*100:.2f}%")
        display_df["Revenue"] = display_df["Revenue"].apply(lambda x: f"${x:,.2f}" if x else "$0.00")

        st.dataframe(display_df, use_container_width=True, hide_index=True)
else:
    st.info("No country data available.")

# Refresh button
st.markdown("---")
if st.button("Refresh Data", key="funnel_refresh"):
    st.cache_data.clear()
    st.rerun()
