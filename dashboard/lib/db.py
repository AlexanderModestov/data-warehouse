"""
Database connection helper for the Stripe Payments Dashboard.
Reads credentials from .env file or Streamlit secrets.
"""

import os
import streamlit as st
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from dotenv import load_dotenv

# Load .env file from project root (two levels up from lib/)
env_path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
load_dotenv(env_path)


def get_database_url() -> str:
    """
    Get database URL from environment variables or Streamlit secrets.
    Priority: DATABASE_URL env var > PG_ANALYTICS_* env vars > Streamlit secrets
    """
    # First check for DATABASE_URL directly
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return database_url

    # Build from PG_ANALYTICS_* environment variables
    host = os.getenv("PG_ANALYTICS_HOST")
    port = os.getenv("PG_ANALYTICS_PORT", "5432")
    user = os.getenv("PG_ANALYTICS_USER")
    password = os.getenv("PG_ANALYTICS_PASSWORD")
    dbname = os.getenv("PG_ANALYTICS_DBNAME")

    if all([host, user, password, dbname]):
        return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

    # Fall back to Streamlit secrets
    return st.secrets.get("DATABASE_URL", "")


@contextmanager
def get_connection():
    """
    Context manager for database connections.

    Usage:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM table")
                results = cur.fetchall()
    """
    database_url = get_database_url()

    if not database_url:
        st.error("Database connection not configured. Set PG_ANALYTICS_* in .env or DATABASE_URL in secrets.")
        st.stop()

    conn = None
    try:
        conn = psycopg2.connect(database_url)
        yield conn
    except psycopg2.OperationalError as e:
        st.error(f"Database connection failed: {str(e)}")
        st.stop()
    finally:
        if conn is not None:
            conn.close()


def execute_query(query: str, params: tuple = None) -> list[dict]:
    """
    Execute a query and return results as a list of dictionaries.

    Args:
        query: SQL query string
        params: Optional tuple of parameters for parameterized query

    Returns:
        List of dictionaries with column names as keys
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            return [dict(row) for row in cur.fetchall()]
