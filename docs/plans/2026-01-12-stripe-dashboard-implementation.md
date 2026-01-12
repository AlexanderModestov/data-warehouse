# Stripe Dashboard Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a Streamlit dashboard showing Stripe payment statistics with mock data, ready to swap in real database queries.

**Architecture:** Two-page Streamlit app (Overview + Explorer) with a data layer abstraction. Mock data module mimics the real query interface so swapping is trivial.

**Tech Stack:** Streamlit, pandas, plotly, Python 3.11

---

## Task 1: Project Structure & Dependencies

**Files:**
- Create: `dashboard/requirements.txt`
- Create: `dashboard/.streamlit/config.toml`

**Step 1: Create dashboard directory structure**

```bash
cd .worktrees/stripe-dashboard
mkdir -p dashboard/pages dashboard/lib dashboard/.streamlit
```

**Step 2: Create requirements.txt**

Create `dashboard/requirements.txt`:

```
streamlit>=1.30.0
pandas>=2.0.0
plotly>=5.18.0
python-dotenv>=1.0.0
```

**Step 3: Create Streamlit config**

Create `dashboard/.streamlit/config.toml`:

```toml
[theme]
primaryColor = "#FF6B6B"
backgroundColor = "#FFFFFF"
secondaryBackgroundColor = "#F0F2F6"
textColor = "#262730"

[server]
headless = true
```

**Step 4: Commit**

```bash
git add dashboard/
git commit -m "feat(dashboard): initialize project structure and dependencies"
```

---

## Task 2: Mock Data Layer

**Files:**
- Create: `dashboard/lib/__init__.py`
- Create: `dashboard/lib/mock_data.py`
- Create: `dashboard/lib/queries.py`

**Step 1: Create empty __init__.py**

Create `dashboard/lib/__init__.py`:

```python
# Dashboard library modules
```

**Step 2: Create mock data generator**

Create `dashboard/lib/mock_data.py`:

```python
"""
Mock data for dashboard development.
Mimics the structure of mart_stripe_payments and mart_stripe_payments_daily.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def generate_mock_payments(days: int = 30, payments_per_day: int = 30) -> pd.DataFrame:
    """Generate mock payment data matching mart_stripe_payments schema."""
    np.random.seed(42)

    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    records = []
    charge_counter = 1

    for day_offset in range(days):
        current_date = start_date + timedelta(days=day_offset)
        num_payments = payments_per_day + np.random.randint(-10, 10)

        for _ in range(num_payments):
            # 94% success rate
            is_successful = np.random.random() < 0.94
            status = "succeeded" if is_successful else "failed"

            # Failure categories for failed payments
            failure_categories = [
                ("card_declined", "request_new_card"),
                ("insufficient_funds", "retry_eligible"),
                ("authentication_required", "verify_3ds"),
                ("fraud_block", "contact_support"),
                ("expired_card", "request_new_card"),
                ("processing_error", "retry_eligible"),
            ]

            if not is_successful:
                failure_cat, recovery = failure_categories[
                    np.random.choice(len(failure_categories), p=[0.35, 0.25, 0.15, 0.10, 0.10, 0.05])
                ]
            else:
                failure_cat, recovery = None, None

            # Random hour of day
            hour = np.random.randint(6, 23)
            created_at = current_date.replace(hour=hour, minute=np.random.randint(0, 59))

            records.append({
                "charge_id": f"ch_{charge_counter:08d}",
                "payment_intent_id": f"pi_{charge_counter // 2:08d}",
                "customer_id": f"cus_{np.random.randint(1000, 9999):04d}",
                "profile_id": f"prof_{np.random.randint(10000, 99999)}",
                "status": status,
                "is_successful": is_successful,
                "amount_usd": float(np.random.choice([29.99, 49.99, 99.99, 149.99], p=[0.3, 0.4, 0.2, 0.1])),
                "currency": "usd",
                "failure_code": failure_cat if not is_successful else None,
                "failure_category": failure_cat,
                "recovery_action": recovery,
                "attempt_number": np.random.choice([1, 2, 3], p=[0.85, 0.12, 0.03]),
                "created_at": created_at,
                "created_date": created_at.date(),
                "hour_of_day": hour,
                "day_of_week": created_at.strftime("%A"),
                "funnel_name": np.random.choice(["main_funnel", "promo_funnel", "referral_funnel"], p=[0.6, 0.25, 0.15]),
                "card_brand": np.random.choice(["visa", "mastercard", "amex"], p=[0.5, 0.35, 0.15]),
                "card_country": np.random.choice(["US", "GB", "DE", "CA", "AU"], p=[0.6, 0.15, 0.1, 0.1, 0.05]),
            })
            charge_counter += 1

    return pd.DataFrame(records)


def generate_mock_daily_summary(payments_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate mock payments into daily summary matching mart_stripe_payments_daily schema."""

    daily = payments_df.groupby(["created_date", "funnel_name"]).agg(
        total_attempts=("charge_id", "count"),
        successful_payments=("is_successful", "sum"),
        failed_payments=("is_successful", lambda x: (~x).sum()),
        gross_revenue_usd=("amount_usd", lambda x: x[payments_df.loc[x.index, "is_successful"]].sum()),
        failed_revenue_usd=("amount_usd", lambda x: x[~payments_df.loc[x.index, "is_successful"]].sum()),
    ).reset_index()

    daily["success_rate"] = daily["successful_payments"] / daily["total_attempts"]
    daily["date"] = daily["created_date"]

    # Add failure breakdown
    failure_counts = payments_df[~payments_df["is_successful"]].groupby(
        ["created_date", "funnel_name", "failure_category"]
    ).size().unstack(fill_value=0).reset_index()

    daily = daily.merge(failure_counts, on=["created_date", "funnel_name"], how="left")

    return daily
```

**Step 3: Create queries interface**

Create `dashboard/lib/queries.py`:

```python
"""
Data queries for dashboard.
Currently uses mock data - swap to real DB queries when marts are ready.
"""

import streamlit as st
import pandas as pd
from datetime import date, timedelta
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
            df["charge_id"].str.lower().str.contains(search_lower) |
            df["customer_id"].str.lower().str.contains(search_lower) |
            df["profile_id"].str.lower().str.contains(search_lower)
        ]

    total_count = len(df)

    # Sort and paginate
    df = df.sort_values("created_at", ascending=False)
    start_idx = (page - 1) * page_size
    df = df.iloc[start_idx:start_idx + page_size]

    return df, total_count


@st.cache_data(ttl=60)
def get_payment_detail(charge_id: str) -> dict:
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


def get_filter_options() -> dict:
    """Get unique values for filter dropdowns."""
    df = _get_mock_payments()
    return {
        "funnels": ["All"] + sorted(df["funnel_name"].unique().tolist()),
        "card_brands": ["All"] + sorted(df["card_brand"].unique().tolist()),
        "statuses": ["All", "Successful", "Failed"],
    }
```

**Step 4: Commit**

```bash
git add dashboard/lib/
git commit -m "feat(dashboard): add mock data layer with query interface"
```

---

## Task 3: Authentication

**Files:**
- Create: `dashboard/app.py`

**Step 1: Create main app with auth**

Create `dashboard/app.py`:

```python
"""
Stripe Payments Dashboard
Entry point with authentication.
"""

import streamlit as st

st.set_page_config(
    page_title="Stripe Payments Dashboard",
    page_icon="💳",
    layout="wide",
    initial_sidebar_state="expanded",
)


def check_password() -> bool:
    """Simple password authentication."""

    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if st.session_state.authenticated:
        return True

    st.title("🔐 Stripe Payments Dashboard")
    st.write("Please enter the dashboard password to continue.")

    password = st.text_input("Password", type="password", key="password_input")

    if st.button("Login", type="primary"):
        # In production, use: st.secrets["DASHBOARD_PASSWORD"]
        # For development, accept any non-empty password
        correct_password = st.secrets.get("DASHBOARD_PASSWORD", "demo")

        if password == correct_password:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("Incorrect password")

    return False


def main():
    if not check_password():
        st.stop()

    # Sidebar
    st.sidebar.title("💳 Payments Dashboard")
    st.sidebar.markdown("---")
    st.sidebar.markdown("Navigate using the pages above.")
    st.sidebar.markdown("---")
    st.sidebar.caption("Data refreshes every 5 minutes")

    # Main content
    st.title("Welcome to the Stripe Payments Dashboard")
    st.markdown("""
    Use the sidebar to navigate between pages:

    - **Overview** - High-level metrics, charts, and failure analysis
    - **Payments** - Detailed payment explorer with filters and search

    ---

    ⚠️ **Development Mode:** Currently showing mock data.
    """)


if __name__ == "__main__":
    main()
```

**Step 2: Create secrets template**

Create `dashboard/.streamlit/secrets.toml.example`:

```toml
# Copy this to secrets.toml and fill in values
# DO NOT commit secrets.toml to git

DASHBOARD_PASSWORD = "your-team-password"

# When ready for real database:
# DATABASE_URL = "postgresql://user:pass@host:5432/dbname"
```

**Step 3: Add secrets to gitignore**

Append to `dashboard/.gitignore`:

```
# Streamlit secrets
.streamlit/secrets.toml
```

**Step 4: Create local secrets for testing**

Create `dashboard/.streamlit/secrets.toml`:

```toml
DASHBOARD_PASSWORD = "demo"
```

**Step 5: Test the app runs**

```bash
cd dashboard
pip install -r requirements.txt
streamlit run app.py
```

Expected: Browser opens, shows login page, "demo" password works.

**Step 6: Commit**

```bash
git add dashboard/app.py dashboard/.streamlit/secrets.toml.example dashboard/.gitignore
git commit -m "feat(dashboard): add authentication and main entry point"
```

---

## Task 4: Overview Page

**Files:**
- Create: `dashboard/pages/1_Overview.py`

**Step 1: Create overview page**

Create `dashboard/pages/1_Overview.py`:

```python
"""
Overview Page - High-level payment metrics and charts.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, timedelta

from lib import queries

# Page config
st.set_page_config(page_title="Overview - Payments", page_icon="📊", layout="wide")

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

    fig.update_layout(
        yaxis=dict(title="Revenue ($)", side="left"),
        yaxis2=dict(title="Success Rate (%)", side="right", overlaying="y", range=[80, 100]),
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
```

**Step 2: Test the overview page**

```bash
cd dashboard
streamlit run app.py
```

Expected: Login with "demo", navigate to Overview, see metrics and charts.

**Step 3: Commit**

```bash
git add dashboard/pages/1_Overview.py
git commit -m "feat(dashboard): add overview page with metrics and charts"
```

---

## Task 5: Payments Explorer Page

**Files:**
- Create: `dashboard/pages/2_Payments.py`

**Step 1: Create payments explorer page**

Create `dashboard/pages/2_Payments.py`:

```python
"""
Payments Explorer - Detailed payment list with filters.
"""

import streamlit as st
import pandas as pd
from datetime import date, timedelta

from lib import queries

# Page config
st.set_page_config(page_title="Payments Explorer", page_icon="🔍", layout="wide")

# Auth check
if not st.session_state.get("authenticated", False):
    st.warning("Please login from the main page.")
    st.stop()

st.title("🔍 Payments Explorer")

# Initialize session state for pagination
if "page" not in st.session_state:
    st.session_state.page = 1

# Get filter options
filter_options = queries.get_filter_options()

# Filters row
st.markdown("### Filters")
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    status_filter = st.selectbox("Status", filter_options["statuses"])

with col2:
    start_date = st.date_input("From", date.today() - timedelta(days=30))

with col3:
    end_date = st.date_input("To", date.today())

with col4:
    funnel_filter = st.selectbox("Funnel", filter_options["funnels"])

with col5:
    card_filter = st.selectbox("Card Brand", filter_options["card_brands"])

# Search row
search = st.text_input("🔍 Search by Charge ID, Customer ID, or Profile ID", "")

# Reset page when filters change
filter_key = f"{status_filter}-{start_date}-{end_date}-{funnel_filter}-{card_filter}-{search}"
if "last_filter_key" not in st.session_state or st.session_state.last_filter_key != filter_key:
    st.session_state.page = 1
    st.session_state.last_filter_key = filter_key

st.markdown("---")

# Get data
page_size = 50
with st.spinner("Loading payments..."):
    payments_df, total_count = queries.get_payments_list(
        start_date=start_date,
        end_date=end_date,
        status=status_filter,
        funnel=funnel_filter,
        card_brand=card_filter,
        search=search,
        page=st.session_state.page,
        page_size=page_size,
    )

# Results header
total_pages = max(1, (total_count + page_size - 1) // page_size)

col1, col2 = st.columns([6, 4])
with col1:
    st.markdown(f"**Showing {len(payments_df)} of {total_count:,} payments**")
with col2:
    # Export button
    if not payments_df.empty:
        csv = payments_df.to_csv(index=False)
        st.download_button(
            label="📥 Export CSV",
            data=csv,
            file_name=f"payments_{start_date}_{end_date}.csv",
            mime="text/csv",
        )

# Display table
if not payments_df.empty:
    # Format for display
    display_df = payments_df[[
        "charge_id", "created_at", "amount_usd", "status",
        "failure_category", "funnel_name", "card_brand"
    ]].copy()

    display_df["created_at"] = pd.to_datetime(display_df["created_at"]).dt.strftime("%Y-%m-%d %H:%M")
    display_df["amount_usd"] = display_df["amount_usd"].apply(lambda x: f"${x:.2f}")
    display_df["status"] = display_df["status"].apply(lambda x: "✓" if x == "succeeded" else "✗")
    display_df["failure_category"] = display_df["failure_category"].fillna("-")

    display_df.columns = ["Charge ID", "Date", "Amount", "Status", "Failure", "Funnel", "Card"]

    # Clickable table using dataframe
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        height=500,
    )

    # Pagination
    st.markdown("---")
    col1, col2, col3 = st.columns([2, 6, 2])

    with col1:
        if st.button("← Previous", disabled=st.session_state.page <= 1):
            st.session_state.page -= 1
            st.rerun()

    with col2:
        st.markdown(f"<center>Page {st.session_state.page} of {total_pages}</center>", unsafe_allow_html=True)

    with col3:
        if st.button("Next →", disabled=st.session_state.page >= total_pages):
            st.session_state.page += 1
            st.rerun()

    # Payment detail expander
    st.markdown("---")
    st.subheader("Payment Details")

    selected_charge = st.selectbox(
        "Select a payment to view details",
        options=[""] + payments_df["charge_id"].tolist(),
        format_func=lambda x: "Choose a payment..." if x == "" else x,
    )

    if selected_charge:
        detail = queries.get_payment_detail(selected_charge)

        if detail:
            col1, col2 = st.columns(2)

            with col1:
                st.markdown(f"""
                **Charge ID:** `{detail['charge_id']}`
                **Customer:** `{detail['customer_id']}`
                **Profile:** `{detail['profile_id']}`
                **Amount:** ${detail['amount_usd']:.2f} {detail['currency'].upper()}
                """)

            with col2:
                status_icon = "✓" if detail["status"] == "succeeded" else "✗"
                st.markdown(f"""
                **Status:** {status_icon} {detail['status']}
                **Card:** {detail['card_brand'].title()} ({detail['card_country']})
                **Failure:** {detail['failure_category'] or '-'}
                **Recovery:** {detail['recovery_action'] or '-'}
                """)

            # Retry history
            if len(detail["retry_history"]) > 1:
                st.markdown("**Retry History:**")
                for i, attempt in enumerate(detail["retry_history"], 1):
                    status_icon = "✓" if attempt["status"] == "succeeded" else "✗"
                    failure = f" ({attempt['failure_category']})" if attempt["failure_category"] else ""
                    st.markdown(f"- Attempt {i}: {attempt['created_at'].strftime('%Y-%m-%d %H:%M')} - {status_icon} {attempt['status']}{failure}")

else:
    st.info("No payments found matching your filters.")

# Refresh button
st.markdown("---")
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()
```

**Step 2: Test the explorer page**

```bash
cd dashboard
streamlit run app.py
```

Expected: Login, navigate to Payments, see filterable table with pagination.

**Step 3: Commit**

```bash
git add dashboard/pages/2_Payments.py
git commit -m "feat(dashboard): add payments explorer page with filters"
```

---

## Task 6: Deployment Files

**Files:**
- Create: `dashboard/Procfile`
- Create: `dashboard/runtime.txt`
- Create: `dashboard/README.md`

**Step 1: Create Heroku Procfile**

Create `dashboard/Procfile`:

```
web: streamlit run app.py --server.port=$PORT --server.address=0.0.0.0
```

**Step 2: Create runtime.txt**

Create `dashboard/runtime.txt`:

```
python-3.11.9
```

**Step 3: Create README**

Create `dashboard/README.md`:

```markdown
# Stripe Payments Dashboard

Streamlit dashboard for viewing Stripe payment statistics.

## Local Development

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Create `.streamlit/secrets.toml`:
   ```toml
   DASHBOARD_PASSWORD = "your-password"
   ```

3. Run the app:
   ```bash
   streamlit run app.py
   ```

## Deployment to Heroku

1. Create Heroku app:
   ```bash
   heroku create your-app-name
   ```

2. Set environment variables:
   ```bash
   heroku config:set DASHBOARD_PASSWORD=your-secure-password
   ```

3. Deploy:
   ```bash
   git subtree push --prefix dashboard heroku main
   ```

## Switching to Real Data

When dbt marts are ready, update `lib/queries.py`:

1. Add database connection in `lib/db.py`
2. Replace mock data calls with SQL queries
3. Update `.streamlit/secrets.toml` with `DATABASE_URL`

## Pages

- **Overview** - Revenue, success rate, failure breakdown
- **Payments** - Detailed payment explorer with filters
```

**Step 4: Commit**

```bash
git add dashboard/Procfile dashboard/runtime.txt dashboard/README.md
git commit -m "feat(dashboard): add deployment files and documentation"
```

---

## Task 7: Final Testing & Cleanup

**Step 1: Full app test**

```bash
cd dashboard
streamlit run app.py
```

Test checklist:
- [ ] Login with "demo" password works
- [ ] Overview page loads with metrics
- [ ] Charts render correctly
- [ ] Payments page loads with data
- [ ] Filters work (status, date, funnel, card)
- [ ] Search works
- [ ] Pagination works
- [ ] CSV export works
- [ ] Payment detail expander works

**Step 2: Verify all files**

```bash
git status
```

Expected: Clean working tree, all changes committed.

**Step 3: Push branch**

```bash
git push -u origin feature/stripe-dashboard
```

---

## Summary

After completing all tasks, you will have:

```
dashboard/
├── app.py                      # Entry point with auth
├── pages/
│   ├── 1_Overview.py           # Metrics & charts
│   └── 2_Payments.py           # Payment explorer
├── lib/
│   ├── __init__.py
│   ├── mock_data.py            # Mock data generator
│   └── queries.py              # Data interface (swap mock→real here)
├── .streamlit/
│   ├── config.toml             # Theme config
│   ├── secrets.toml            # Local secrets (not committed)
│   └── secrets.toml.example    # Template
├── .gitignore
├── requirements.txt
├── Procfile
├── runtime.txt
└── README.md
```

**To swap to real data later:**
1. Add `psycopg2-binary` or `sqlalchemy` to requirements.txt (already there)
2. Create `lib/db.py` with connection helper
3. Update functions in `lib/queries.py` to use SQL instead of mock data
