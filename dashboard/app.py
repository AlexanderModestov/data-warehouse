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

    # Logout button
    if st.sidebar.button("Logout"):
        st.session_state.authenticated = False
        st.rerun()

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
