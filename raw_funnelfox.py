import os
import json
import time
from pathlib import Path

import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

BASE_URL = "https://api.funnelfox.io/public/v1"
# Секретный ключ проекта — положите его в переменную окружения
FOX_SECRET = os.environ.get("FUNNEL_FOX_API")
print(FOX_SECRET)

if not FOX_SECRET:
    raise RuntimeError("Положите Fox-Secret в переменную окружения FUNNELFOX_SECRET")

HEADERS = {
    "Fox-Secret": FOX_SECRET,
}

OUTPUT_DIR = Path("raw_funnelfox")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CURSOR_FILE = OUTPUT_DIR / "cursors.json"


def load_cursors() -> dict:
    """Load saved cursors from file."""
    if CURSOR_FILE.exists():
        with CURSOR_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cursor(endpoint: str, cursor: str | None, count: int, offset: int | None = None):
    """Save cursor/offset for an endpoint to resume later."""
    cursors = load_cursors()
    state = {"count": count}
    if cursor:
        state["cursor"] = cursor
    if offset is not None:
        state["offset"] = offset
    cursors[endpoint] = state
    with CURSOR_FILE.open("w", encoding="utf-8") as f:
        json.dump(cursors, f, indent=2)


def clear_cursor(endpoint: str):
    """Clear saved cursor for an endpoint after successful completion."""
    cursors = load_cursors()
    if endpoint in cursors:
        del cursors[endpoint]
        with CURSOR_FILE.open("w", encoding="utf-8") as f:
            json.dump(cursors, f, indent=2)

# PostgreSQL connection parameters
PG_CONFIG = {
    "host": os.environ.get("PG_ANALYTICS_HOST"),
    "user": os.environ.get("PG_ANALYTICS_USER"),
    "password": os.environ.get("PG_ANALYTICS_PASSWORD"),
    "dbname": os.environ.get("PG_ANALYTICS_DBNAME"),
    "port": os.environ.get("PG_ANALYTICS_PORT", "5432"),
}


def fetch_page(endpoint: str, params: dict, max_retries: int = 5) -> dict | None:
    """
    Fetch a single page from the API with retry logic.
    Returns the JSON response or None if all retries failed.
    """
    url = f"{BASE_URL}/{endpoint}"
    resp = None

    for retry in range(max_retries):
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=120)
            resp.raise_for_status()
            _ = resp.content  # Force read to catch chunked encoding errors
            return resp.json()
        except (
            requests.exceptions.Timeout,
            requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
        ) as e:
            if isinstance(e, requests.exceptions.HTTPError) and resp is not None:
                if resp.status_code not in [408, 429, 500, 502, 503, 504, 524]:
                    raise

            if retry < max_retries - 1:
                wait_time = min(5 * (2 ** retry), 60)
                if isinstance(e, requests.exceptions.ChunkedEncodingError):
                    error_msg = "connection broken"
                elif isinstance(e, requests.exceptions.HTTPError) and resp is not None:
                    error_msg = f"{resp.status_code} error"
                elif isinstance(e, requests.exceptions.ConnectionError):
                    error_msg = "connection error"
                else:
                    error_msg = "timeout"
                print(f"    {error_msg}, waiting {wait_time}s... (attempt {retry + 2}/{max_retries})")
                time.sleep(wait_time)
            else:
                return None
    return None


def fetch_all(endpoint: str, params: dict | None = None, conn=None, insert_func=None, resume: bool = True) -> list[dict]:
    """
    Универсальная функция выгрузки всех страниц для list-эндпоинта.
    endpoint: например, 'funnels', 'products', 'sessions', 'subscriptions'
    conn: optional database connection for incremental insertion
    insert_func: optional function to insert data incrementally
    resume: whether to resume from saved cursor
    """
    if params is None:
        params = {}

    items: list[dict] = []
    cursor = None
    page_num = 0
    base_page_delay = 5.0  # Increased delay to avoid 408 timeouts
    total_count = 0
    completed = False
    # Use smaller limit to avoid 408 timeouts on heavy endpoints like sessions
    limit = 50

    # FunnelFox API uses cursor-based pagination only (no offset support)
    # First probe to get total count for progress reporting
    print(f"  Checking total count...")
    probe = fetch_page(endpoint, {"limit": 1})
    api_total = None
    if probe:
        pagination = probe.get("pagination") or {}
        api_total = pagination.get("total")
        if api_total:
            print(f"  API reports {api_total} total items")

    # Check for saved state to resume (for cursor-based pagination)
    if resume:
        cursors = load_cursors()
        if endpoint in cursors:
            saved = cursors[endpoint]
            total_count = saved.get("count", 0)
            if "cursor" in saved and saved["cursor"]:
                cursor = saved["cursor"]
                print(f"  Resuming from cursor (previously loaded {total_count} items)")

    # Use cursor-based pagination
    print(f"  Using cursor pagination (limit={limit})...")

    while True:
        page_num += 1
        query = params.copy()
        query["limit"] = limit

        if cursor:
            query["cursor"] = cursor

        data = fetch_page(endpoint, query)

        if data is None:
            print(f"  Failed on page {page_num} after all retries")
            print(f"  Successfully loaded {len(items)} items this run ({total_count + len(items)} total)")
            if cursor:
                save_cursor(endpoint, cursor, total_count + len(items))
                print(f"  Progress saved. Run again to resume.")
            break

        page_items = data.get("data", [])
        items.extend(page_items)

        pagination = data.get("pagination") or {}
        has_more = pagination.get("has_more")
        next_cursor = pagination.get("next_cursor") or pagination.get("cursor")
        api_total = pagination.get("total")

        print(f"  Page {page_num}: loaded {len(page_items)} items, has_more: {has_more}, total so far: {total_count + len(items)}" +
              (f" (API total: {api_total})" if api_total else ""))

        if conn and insert_func and page_items:
            try:
                insert_func(conn, page_items)
                print(f"  -> Inserted page {page_num} to database")
            except Exception as e:
                print(f"  Warning: Failed to insert page {page_num}: {e}")

        # Save cursor after each successful page
        if next_cursor and has_more:
            save_cursor(endpoint, next_cursor, total_count + len(items))

        if not has_more:
            completed = True
            break

        if not next_cursor:
            print(f"  WARNING: No cursor provided but has_more=True. Stopping.")
            break

        cursor = next_cursor

        # Adaptive delay
        delay = base_page_delay + (page_num // 10)
        if page_num % 10 == 0:
            print(f"  [Throttling] Pausing {delay}s after {page_num} pages...")
        time.sleep(delay)

    # Clear saved state on successful completion
    if completed:
        clear_cursor(endpoint)
        print(f"  Completed! Total items: {total_count + len(items)}")

    return items


def save_json(name: str, data: list[dict]) -> None:
    path = OUTPUT_DIR / f"{name}.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(data)} items to {path}")


def get_db_connection():
    """Create and return a PostgreSQL connection."""
    return psycopg2.connect(**PG_CONFIG)


def get_latest_timestamp(conn, table: str, column: str = "created_at") -> str | None:
    """
    Get the latest timestamp from a table for incremental loading.
    Returns ISO format string or None if table is empty.
    """
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX({column}) FROM raw_funnelfox.{table}")
        result = cur.fetchone()[0]
        if result:
            return result.isoformat()
    return None


def get_record_count(conn, table: str) -> int:
    """Get the count of records in a table."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM raw_funnelfox.{table}")
        return cur.fetchone()[0]


def init_schema(conn):
    """Initialize database schema and tables."""
    with conn.cursor() as cur:
        # Read and execute schema file
        schema_path = Path(__file__).parent / "schemas" / "funnelfox_schema.sql"
        with schema_path.open("r", encoding="utf-8") as f:
            schema_sql = f.read()
        cur.execute(schema_sql)
    conn.commit()
    print("Database schema initialized")


def insert_funnels(conn, data: list[dict]) -> None:
    """Insert funnels data into PostgreSQL."""
    if not data:
        return

    with conn.cursor() as cur:
        # Use ON CONFLICT to handle duplicates
        insert_query = """
            INSERT INTO raw_funnelfox.funnels
            (id, alias, environment, last_published_at, status, tags, title, type, variation_count, version)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                alias = EXCLUDED.alias,
                environment = EXCLUDED.environment,
                last_published_at = EXCLUDED.last_published_at,
                status = EXCLUDED.status,
                tags = EXCLUDED.tags,
                title = EXCLUDED.title,
                type = EXCLUDED.type,
                variation_count = EXCLUDED.variation_count,
                version = EXCLUDED.version,
                loaded_at = CURRENT_TIMESTAMP
        """
        values = [
            (
                item.get("id"),
                item.get("alias"),
                item.get("environment"),
                item.get("last_published_at"),
                item.get("status"),
                item.get("tags", []),
                item.get("title"),
                item.get("type"),
                item.get("variation_count"),
                item.get("version"),
            )
            for item in data
        ]
        execute_values(cur, insert_query, values)
    conn.commit()
    print(f"Inserted {len(data)} funnels into database")


def insert_products(conn, data: list[dict]) -> None:
    """Insert products data into PostgreSQL."""
    if not data:
        return

    with conn.cursor() as cur:
        insert_query = """
            INSERT INTO raw_funnelfox.products (id, data)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                data = EXCLUDED.data,
                loaded_at = CURRENT_TIMESTAMP
        """
        values = [
            (item.get("id"), json.dumps(item))
            for item in data
        ]
        execute_values(cur, insert_query, values)
    conn.commit()
    print(f"Inserted {len(data)} products into database")


def insert_sessions(conn, data: list[dict]) -> None:
    """Insert sessions data into PostgreSQL."""
    if not data:
        return

    with conn.cursor() as cur:
        insert_query = """
            INSERT INTO raw_funnelfox.sessions
            (id, city, country, created_at, funnel_id, funnel_version, ip, origin, postal, profile_id, user_agent)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                city = EXCLUDED.city,
                country = EXCLUDED.country,
                created_at = EXCLUDED.created_at,
                funnel_id = EXCLUDED.funnel_id,
                funnel_version = EXCLUDED.funnel_version,
                ip = EXCLUDED.ip,
                origin = EXCLUDED.origin,
                postal = EXCLUDED.postal,
                profile_id = EXCLUDED.profile_id,
                user_agent = EXCLUDED.user_agent,
                loaded_at = CURRENT_TIMESTAMP
        """
        values = [
            (
                item.get("id"),
                item.get("city"),
                item.get("country"),
                item.get("created_at"),
                item.get("funnel_id"),
                item.get("funnel_version"),
                item.get("ip"),
                item.get("origin"),
                item.get("postal"),
                item.get("profile_id"),
                item.get("user_agent"),
            )
            for item in data
        ]
        execute_values(cur, insert_query, values)
    conn.commit()
    print(f"Inserted {len(data)} sessions into database")


def insert_subscriptions(conn, data: list[dict]) -> None:
    """Insert subscriptions data into PostgreSQL."""
    if not data:
        return

    with conn.cursor() as cur:
        insert_query = """
            INSERT INTO raw_funnelfox.subscriptions
            (id, billing_interval, billing_interval_count, created_at, currency, funnel_version,
             payment_provider, period_ends_at, period_starts_at, price, price_usd, profile_id, psp_id,
             renews, sandbox, status, updated_at)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                billing_interval = EXCLUDED.billing_interval,
                billing_interval_count = EXCLUDED.billing_interval_count,
                created_at = EXCLUDED.created_at,
                currency = EXCLUDED.currency,
                funnel_version = EXCLUDED.funnel_version,
                payment_provider = EXCLUDED.payment_provider,
                period_ends_at = EXCLUDED.period_ends_at,
                period_starts_at = EXCLUDED.period_starts_at,
                price = EXCLUDED.price,
                price_usd = EXCLUDED.price_usd,
                profile_id = EXCLUDED.profile_id,
                psp_id = EXCLUDED.psp_id,
                renews = EXCLUDED.renews,
                sandbox = EXCLUDED.sandbox,
                status = EXCLUDED.status,
                updated_at = EXCLUDED.updated_at,
                loaded_at = CURRENT_TIMESTAMP
        """
        values = [
            (
                item.get("id"),
                item.get("billing_interval"),
                item.get("billing_interval_count"),
                item.get("created_at"),
                item.get("currency"),
                item.get("funnel_version"),
                item.get("payment_provider"),
                item.get("period_ends_at"),
                item.get("period_starts_at"),
                item.get("price"),
                item.get("price_usd"),
                # API returns profile as nested object: {"profile": {"id": "..."}}
                item.get("profile", {}).get("id") if isinstance(item.get("profile"), dict) else item.get("profile_id"),
                item.get("psp_id"),
                item.get("renews"),
                item.get("sandbox"),
                item.get("status"),
                item.get("updated_at"),
            )
            for item in data
        ]
        execute_values(cur, insert_query, values)
    conn.commit()
    print(f"Inserted {len(data)} subscriptions into database")


def insert_profiles(conn, data: list[dict]) -> None:
    """Insert profiles data into PostgreSQL."""
    if not data:
        return

    with conn.cursor() as cur:
        insert_query = """
            INSERT INTO raw_funnelfox.profiles (id, data)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                data = EXCLUDED.data,
                loaded_at = CURRENT_TIMESTAMP
        """
        values = [
            (item.get("id"), json.dumps(item))
            for item in data
        ]
        execute_values(cur, insert_query, values)
    conn.commit()
    print(f"Inserted {len(data)} profiles into database")


def insert_transactions(conn, data: list[dict]) -> None:
    """Insert transactions data into PostgreSQL."""
    if not data:
        return

    with conn.cursor() as cur:
        insert_query = """
            INSERT INTO raw_funnelfox.transactions (id, data)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                data = EXCLUDED.data,
                loaded_at = CURRENT_TIMESTAMP
        """
        values = [
            (item.get("id"), json.dumps(item))
            for item in data
        ]
        execute_values(cur, insert_query, values)
    conn.commit()
    print(f"Inserted {len(data)} transactions into database")


def insert_session_replies(conn, session_id: str, data: list[dict]) -> None:
    """Insert session replies data into PostgreSQL."""
    if not data:
        return

    with conn.cursor() as cur:
        insert_query = """
            INSERT INTO raw_funnelfox.session_replies (id, session_id, data)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                session_id = EXCLUDED.session_id,
                data = EXCLUDED.data,
                loaded_at = CURRENT_TIMESTAMP
        """
        values = [
            (item.get("id"), session_id, json.dumps(item))
            for item in data
        ]
        execute_values(cur, insert_query, values)
    conn.commit()


def fetch_session_replies(conn, sessions: list[dict]) -> int:
    """
    Fetch replies for all sessions.
    Returns total number of replies fetched.
    """
    total_replies = 0
    total_sessions = len(sessions)
    sessions_with_replies = 0
    max_retries = 5
    base_delay = 1.0  # 1 second between requests

    print(f"\nFetching replies for {total_sessions} sessions...")

    for idx, session in enumerate(sessions, 1):
        session_id = session.get("id")
        if not session_id:
            continue

        url = f"{BASE_URL}/sessions/{session_id}/replies"

        # Progress indicator every 100 sessions
        if idx % 100 == 0 or idx == total_sessions:
            print(f"  Progress: {idx}/{total_sessions} sessions processed, {total_replies} replies found")

        # Retry logic
        resp = None
        for retry in range(max_retries):
            try:
                resp = requests.get(url, headers=HEADERS, timeout=60)
                if resp.status_code == 404:
                    # No replies for this session
                    break
                resp.raise_for_status()
                break
            except (
                requests.exceptions.Timeout,
                requests.exceptions.HTTPError,
                requests.exceptions.ConnectionError,
            ) as e:
                if isinstance(e, requests.exceptions.HTTPError) and resp is not None:
                    if resp.status_code == 404:
                        break
                    if resp.status_code not in [408, 429, 500, 502, 503, 504]:
                        break

                if retry < max_retries - 1:
                    wait_time = min(2 * (2 ** retry), 30)
                    print(f"  Error on session {session_id}, waiting {wait_time}s... (attempt {retry + 2}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    print(f"  Failed to fetch replies for session {session_id} after {max_retries} retries")
                    resp = None

        if resp is None or resp.status_code == 404:
            time.sleep(base_delay * 0.5)  # Shorter delay for 404s
            continue

        try:
            data = resp.json()
            # API may return list directly or wrapped in {"data": [...]}
            if isinstance(data, list):
                replies = data
            else:
                replies = data.get("data", [])

            if replies:
                insert_session_replies(conn, session_id, replies)
                total_replies += len(replies)
                sessions_with_replies += 1

        except Exception as e:
            print(f"  Error processing replies for session {session_id}: {e}")

        # Rate limiting
        time.sleep(base_delay)

    print(f"\nSession replies complete: {total_replies} replies from {sessions_with_replies} sessions")
    return total_replies


def main():
    import argparse
    parser = argparse.ArgumentParser(description="FunnelFox data loader")
    parser.add_argument("--reset", action="store_true", help="Clear saved cursors and start fresh")
    parser.add_argument("--skip-replies", action="store_true", help="Skip fetching session replies")
    parser.add_argument("--only", type=str, help="Only export specific endpoint (sessions, subscriptions, etc.)")
    args = parser.parse_args()

    # Clear cursors if requested
    if args.reset:
        if CURSOR_FILE.exists():
            CURSOR_FILE.unlink()
            print("Cursors cleared.")

    # Что именно выгружать — можно менять список
    endpoints = {
        "funnels": "funnels",              # список всех воронок
        "products": "products",            # продукты
        "sessions": "sessions",            # сессии пользователей
        "subscriptions": "subscriptions",  # подписки
        "profiles": "profiles",            # профили пользователей
        "transactions": "transactions",    # транзакции
    }

    # Filter to specific endpoint if requested
    if args.only:
        if args.only not in endpoints:
            print(f"Error: Unknown endpoint '{args.only}'")
            print(f"Available: {', '.join(endpoints.keys())}")
            return
        endpoints = {args.only: endpoints[args.only]}

    # Database insertion functions mapping
    insert_functions = {
        "funnels": insert_funnels,
        "products": insert_products,
        "sessions": insert_sessions,
        "subscriptions": insert_subscriptions,
        "profiles": insert_profiles,
        "transactions": insert_transactions,
    }

    # Connect to database
    print("Connecting to PostgreSQL...")
    conn = get_db_connection()

    try:
        # Initialize schema
        init_schema(conn)

        sessions_data = []  # Store sessions for fetching replies later

        # Endpoint-specific params
        endpoint_params = {
            "funnels": {"filter[deleted]": "true"},  # Include deleted funnels for FK integrity
        }

        for name, endpoint in endpoints.items():
            print(f"\nExporting {name} ...")

            insert_func = insert_functions.get(name)
            params = endpoint_params.get(name, {})

            # Check for existing cursor
            cursors = load_cursors()
            if endpoint in cursors:
                print(f"  Found saved cursor - will resume from where we left off")

            # Fetch with cursor-based resumability
            data = fetch_all(endpoint, params=params, conn=conn, insert_func=insert_func, resume=True)

            # Save to JSON (backup)
            if data:
                save_json(name, data)
            else:
                print(f"  No new data to save for {name}")

            # Keep sessions data for replies fetching
            if name == "sessions":
                sessions_data = data

        # Fetch session replies (requires individual API calls per session)
        if sessions_data and not args.skip_replies:
            print("\nExporting session_replies ...")
            fetch_session_replies(conn, sessions_data)
        elif args.skip_replies:
            print("\nSkipping session replies (--skip-replies flag)")

        print("\nAll data successfully loaded into PostgreSQL!")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()
        print("Database connection closed")


if __name__ == "__main__":
    main()
