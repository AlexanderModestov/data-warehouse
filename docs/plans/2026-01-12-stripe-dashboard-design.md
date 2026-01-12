# Stripe Payments Dashboard Design

**Date:** 2026-01-12
**Status:** Approved

## Overview

A web dashboard for viewing Stripe payment statistics from dbt data marts. Built with Streamlit for simplicity, hosted on Heroku for small team access.

## Requirements

- Web-based dashboard accessible by 2-5 team members
- Simple shared password authentication
- Two views: Overview (summary metrics) and Payment Explorer (detailed table)
- Minimal frontend complexity
- Direct PostgreSQL connection to dbt marts

## Tech Stack

- **Streamlit** - Dashboard framework
- **psycopg2 / SQLAlchemy** - PostgreSQL connection
- **pandas** - Data manipulation
- **plotly** - Charts
- **Heroku** - Hosting (~$7/month)

## Project Structure

```
dashboard/
├── app.py                 # Entry point, auth, page config
├── pages/
│   ├── 1_Overview.py      # Summary metrics & charts
│   └── 2_Payments.py      # Detailed payment explorer
├── lib/
│   ├── db.py              # Database connection helper
│   └── queries.py         # SQL queries as functions
├── requirements.txt
├── Procfile               # Heroku deployment
├── runtime.txt            # Python version
└── .streamlit/
    └── secrets.toml       # Local secrets (not committed)
```

## Page 1: Overview

At-a-glance health of Stripe payments.

### Layout

```
┌─────────────────────────────────────────────────────────┐
│  Date Range Picker: [Last 7 days ▼]  [Custom range]    │
├─────────────┬─────────────┬─────────────┬──────────────┤
│  Total Rev  │  Success    │  Failed     │  Attempts    │
│  $12,450    │  Rate: 94%  │  $820       │  847         │
│  ▲ 12%      │  ▲ 2%       │  ▼ 5%       │  ▲ 8%        │
├─────────────┴─────────────┴─────────────┴──────────────┤
│  [Daily Revenue & Success Rate Chart - Line/Bar combo] │
│                                                        │
├────────────────────────────┬───────────────────────────┤
│  Failure Breakdown (Pie)   │  Recovery Rate Trend      │
│  - Card declined: 45%      │  [Line chart]             │
│  - Insufficient funds: 30% │                           │
│  - Auth required: 15%      │  Current: 23%             │
│  - Other: 10%              │                           │
├────────────────────────────┴───────────────────────────┤
│  Top Failure Reasons (Table)                           │
│  Category          | Count | Lost Rev | Action        │
│  card_declined     | 24    | $480     | request_card  │
│  insufficient_funds| 18    | $290     | retry_eligible│
└─────────────────────────────────────────────────────────┘
```

### Metrics (from `mart_stripe_payments_daily`)

- Gross revenue, success rate, failed revenue, total attempts
- Comparison to previous period (% change)
- Failure breakdown by category
- Recovery rate for retried payments

## Page 2: Payments Explorer

Drill into individual payments.

### Layout

```
┌─────────────────────────────────────────────────────────┐
│  Filters                                                │
│  ┌──────────────┐ ┌──────────────┐ ┌────────────────┐  │
│  │ Status: All ▼│ │ Date range   │ │ Search charge  │  │
│  └──────────────┘ └──────────────┘ │ or customer ID │  │
│  ┌──────────────┐ ┌──────────────┐ └────────────────┘  │
│  │ Funnel: All ▼│ │ Card brand ▼ │                     │
│  └──────────────┘ └──────────────┘                     │
├─────────────────────────────────────────────────────────┤
│  Showing 247 payments                    Export CSV [↓] │
├─────────────────────────────────────────────────────────┤
│  Charge ID    │ Date       │ Amount │ Status  │ Fail   │
│  ─────────────┼────────────┼────────┼─────────┼─────── │
│  ch_3Nq...    │ Jan 12     │ $49.00 │ ✓       │ -      │
│  ch_3Np...    │ Jan 12     │ $49.00 │ ✗       │ card_  │
│  ch_3No...    │ Jan 11     │ $99.00 │ ✓       │ -      │
├─────────────────────────────────────────────────────────┤
│  Pagination: [< Prev]  Page 1 of 25  [Next >]          │
└─────────────────────────────────────────────────────────┘
```

### Expandable Row Detail

```
┌─────────────────────────────────────────────────────────┐
│  Charge: ch_3Np...                                      │
│  ───────────────────────────────────────────────────    │
│  Customer: cus_abc123    Profile: prof_xyz789           │
│  Amount: $49.00 USD      Card: Visa ****4242 (US)       │
│  Status: Failed          Failure: card_declined         │
│  Recovery: retry_eligible                               │
│  ───────────────────────────────────────────────────    │
│  Retry History (payment_intent: pi_xxx)                 │
│  Attempt 1: Jan 12 10:23 - Failed (card_declined)       │
│  Attempt 2: Jan 12 10:45 - Succeeded ✓                  │
└─────────────────────────────────────────────────────────┘
```

### Features

- Multi-filter support (all filters combine with AND)
- Server-side pagination (don't load all rows)
- CSV export for offline analysis
- Expandable row detail with retry history

### Data Source

`mart_stripe_payments` (detailed fact table)

## Authentication

Simple shared password using Streamlit's session state:

```python
# app.py
import streamlit as st

def check_password():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        password = st.text_input("Password", type="password")
        if password == st.secrets["DASHBOARD_PASSWORD"]:
            st.session_state.authenticated = True
            st.rerun()
        elif password:
            st.error("Incorrect password")
        st.stop()

check_password()
```

## Deployment

### Platform

Heroku (~$7/month for basic dyno)

### Required Files

**Procfile:**
```
web: streamlit run app.py --server.port=$PORT --server.address=0.0.0.0
```

**runtime.txt:**
```
python-3.11.x
```

**requirements.txt:**
```
streamlit>=1.30.0
psycopg2-binary>=2.9.9
pandas>=2.0.0
plotly>=5.18.0
sqlalchemy>=2.0.0
```

### Environment Variables

| Variable | Purpose |
|----------|---------|
| `DATABASE_URL` | PostgreSQL connection string |
| `DASHBOARD_PASSWORD` | Shared team password |

### Deployment Commands

```bash
heroku create reluvia-dashboard
heroku config:set DATABASE_URL=postgresql://...
heroku config:set DASHBOARD_PASSWORD=your-team-password
git push heroku main
```

### Database Security

- Whitelist Heroku's IP ranges in PostgreSQL firewall
- Use a read-only database user for the dashboard
- Connection string stored in env vars, never in code

## Performance

### Query Caching

```python
# lib/queries.py
import streamlit as st

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_daily_summary(start_date, end_date):
    # Query mart_stripe_payments_daily
    ...

@st.cache_data(ttl=60)   # Cache for 1 minute
def get_payments_list(filters, page, page_size=50):
    # Query mart_stripe_payments with pagination
    ...
```

### TTL Strategy

- Overview metrics: 5 minutes (doesn't need to be real-time)
- Payment explorer: 1 minute (users expect fresher data when searching)
- Manual refresh button available on both pages

### Error Handling

```python
# lib/db.py
def get_connection():
    try:
        return psycopg2.connect(st.secrets["DATABASE_URL"])
    except Exception as e:
        st.error("Database connection failed. Please try again later.")
        st.stop()
```

- Database errors show user-friendly message, not stack traces
- Graceful degradation: if one query fails, other sections still render

### Loading States

```python
with st.spinner("Loading payments..."):
    data = get_payments_list(filters, page)
```

## Data Dependencies

- `mart_stripe_payments` - Detailed payment fact table
- `mart_stripe_payments_daily` - Aggregated daily summary

## Future Enhancements

- Add more data marts (funnel conversions, subscriptions)
- Individual user logins if team grows
- Alerting for sudden failure rate spikes
