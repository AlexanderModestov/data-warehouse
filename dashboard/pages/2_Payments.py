"""
Payments Explorer - Detailed payment list with filters.
"""

import streamlit as st
import pandas as pd
from datetime import date, timedelta

from lib import queries

# Auth check
if not st.session_state.get("authenticated", False):
    st.warning("Please login from the main page.")
    st.stop()

st.title("Payments Explorer")

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
search = st.text_input("Search by Charge ID, Customer ID, or Profile ID", "")

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
            label="Export CSV",
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
    display_df["status"] = display_df["status"].apply(lambda x: "Success" if x == "succeeded" else "Failed")
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
        if st.button("Previous", disabled=st.session_state.page <= 1):
            st.session_state.page -= 1
            st.rerun()

    with col2:
        st.markdown(f"<center>Page {st.session_state.page} of {total_pages}</center>", unsafe_allow_html=True)

    with col3:
        if st.button("Next", disabled=st.session_state.page >= total_pages):
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
                status_text = "Success" if detail["status"] == "succeeded" else "Failed"
                st.markdown(f"""
                **Status:** {status_text}
                **Card:** {detail['card_brand'].title()} ({detail['card_country']})
                **Failure:** {detail['failure_category'] or '-'}
                **Recovery:** {detail['recovery_action'] or '-'}
                """)

            # Retry history
            if len(detail["retry_history"]) > 1:
                st.markdown("**Retry History:**")
                for i, attempt in enumerate(detail["retry_history"], 1):
                    status_icon = "Success" if attempt["status"] == "succeeded" else "Failed"
                    failure = f" ({attempt['failure_category']})" if attempt["failure_category"] else ""
                    st.markdown(f"- Attempt {i}: {attempt['created_at'].strftime('%Y-%m-%d %H:%M')} - {status_icon}{failure}")

else:
    st.info("No payments found matching your filters.")

# Refresh button
st.markdown("---")
if st.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()
