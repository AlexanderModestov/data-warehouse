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

# PostgreSQL connection parameters
PG_CONFIG = {
    "host": os.environ.get("PG_ANALYTICS_HOST"),
    "user": os.environ.get("PG_ANALYTICS_USER"),
    "password": os.environ.get("PG_ANALYTICS_PASSWORD"),
    "dbname": os.environ.get("PG_ANALYTICS_DBNAME"),
    "port": os.environ.get("PG_ANALYTICS_PORT", "5432"),
}


def fetch_all(endpoint: str, params: dict | None = None, conn=None, insert_func=None) -> list[dict]:
    """
    Универсальная функция выгрузки всех страниц для list-эндпоинта.
    endpoint: например, 'funnels', 'products', 'sessions', 'subscriptions'
    conn: optional database connection for incremental insertion
    insert_func: optional function to insert data incrementally
    """
    if params is None:
        params = {}

    items: list[dict] = []
    cursor = None
    offset = 0
    page_num = 0
    max_retries = 10  # Больше попыток
    base_page_delay = 3.0  # Базовая задержка 3 секунды между страницами
    use_offset_pagination = False

    while True:
        page_num += 1
        query = params.copy()
        limit = query.get("limit", 50)  # Уменьшили с 100 до 50 (рекомендация API)
        query["limit"] = limit

        if use_offset_pagination:
            query["offset"] = offset
        elif cursor:
            query["cursor"] = cursor

        url = f"{BASE_URL}/{endpoint}"

        # Retry logic with aggressive backoff
        resp = None
        for retry in range(max_retries):
            try:
                resp = requests.get(url, headers=HEADERS, params=query, timeout=180)
                resp.raise_for_status()
                # Force read content to catch chunked encoding errors
                _ = resp.content
                break
            except (
                requests.exceptions.Timeout,
                requests.exceptions.HTTPError,
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError,  # Сервер обрывает соединение
            ) as e:
                if isinstance(e, requests.exceptions.HTTPError) and resp is not None and resp.status_code not in [408, 429, 500, 502, 503, 504, 524]:
                    raise

                if retry < max_retries - 1:
                    # Агрессивный backoff: 5, 10, 20, 40, 60, 60, 60...
                    wait_time = min(5 * (2 ** retry), 60)
                    if isinstance(e, requests.exceptions.ChunkedEncodingError):
                        error_msg = "connection broken (chunked)"
                    elif isinstance(e, requests.exceptions.HTTPError) and resp is not None:
                        error_msg = f"{resp.status_code} error"
                    elif isinstance(e, requests.exceptions.ConnectionError):
                        error_msg = "connection error"
                    else:
                        error_msg = "timeout"
                    print(f"  {error_msg} on page {page_num}, waiting {wait_time}s... (attempt {retry + 2}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    print(f"  Failed after {max_retries} retries, stopping pagination here")
                    print(f"  Successfully loaded {len(items)} items before stopping")
                    return items

        data = resp.json()
        page_items = data.get("data", [])
        items.extend(page_items)

        pagination = data.get("pagination") or {}
        has_more = pagination.get("has_more")
        cursor = pagination.get("next_cursor") or pagination.get("cursor")

        print(f"  Page {page_num}: loaded {len(page_items)} items, has_more: {has_more}, total so far: {len(items)}")

        if conn and insert_func and page_items:
            try:
                insert_func(conn, page_items)
                print(f"  -> Inserted page {page_num} to database")
            except Exception as e:
                print(f"  Warning: Failed to insert page {page_num}: {e}")

        if not has_more:
            break

        if has_more and not cursor and not use_offset_pagination:
            print(f"  WARNING: No cursor provided. Switching to offset pagination...")
            use_offset_pagination = True
            offset = len(items)
            time.sleep(base_page_delay)
            continue

        if use_offset_pagination:
            offset += limit
            if len(page_items) < limit:
                print(f"  Received {len(page_items)} items (less than limit {limit}), stopping.")
                break

        # Адаптивная задержка: каждые 10 страниц увеличиваем паузу
        adaptive_delay = base_page_delay + (page_num // 10)
        if page_num % 10 == 0:
            print(f"  [Throttling] Pausing {adaptive_delay}s after {page_num} pages...")
        time.sleep(adaptive_delay)

    return items


def save_json(name: str, data: list[dict]) -> None:
    path = OUTPUT_DIR / f"{name}.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(data)} items to {path}")


def get_db_connection():
    """Create and return a PostgreSQL connection."""
    return psycopg2.connect(**PG_CONFIG)


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
                item.get("profile_id"),
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
    # Что именно выгружать — можно менять список
    endpoints = {
        "funnels": "funnels",              # список всех воронок
        "products": "products",            # продукты
        "sessions": "sessions",            # сессии пользователей
        "subscriptions": "subscriptions",  # подписки
        "profiles": "profiles",            # профили пользователей
        "transactions": "transactions",    # транзакции
    }

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

        for name, endpoint in endpoints.items():
            print(f"\nExporting {name} ...")

            # Fetch with incremental insertion
            insert_func = insert_functions.get(name)
            data = fetch_all(endpoint, conn=conn, insert_func=insert_func)

            # Save to JSON (backup)
            save_json(name, data)

            # Keep sessions data for replies fetching
            if name == "sessions":
                sessions_data = data

        # Fetch session replies (requires individual API calls per session)
        if sessions_data:
            print("\nExporting session_replies ...")
            fetch_session_replies(conn, sessions_data)

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
