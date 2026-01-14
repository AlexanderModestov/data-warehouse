"""
Script to validate successful payments between production invoices table
and analytics database mart_stripe_payments.
"""
import os
import psycopg2
from dotenv import load_dotenv
from decimal import Decimal

# Load environment variables from meltano .env
load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'meltano', '.env'))

def get_production_connection():
    """Connect to production database."""
    return psycopg2.connect(
        host=os.getenv('PG_PROD_HOST'),
        port=os.getenv('PG_PROD_PORT'),
        user=os.getenv('PG_PROD_USER'),
        password=os.getenv('PG_PROD_PASSWORD'),
        dbname=os.getenv('PG_PROD_DBNAME'),
        sslmode='require'
    )

def get_analytics_connection():
    """Connect to analytics database."""
    return psycopg2.connect(
        host=os.getenv('PG_ANALYTICS_HOST'),
        port=os.getenv('PG_ANALYTICS_PORT'),
        user=os.getenv('PG_ANALYTICS_USER'),
        password=os.getenv('PG_ANALYTICS_PASSWORD'),
        dbname=os.getenv('PG_ANALYTICS_DBNAME'),
        sslmode='require'
    )

def explore_database_structure():
    """Explore all tables in production database."""
    print("=" * 60)
    print("EXPLORING PRODUCTION DATABASE STRUCTURE")
    print("=" * 60)

    conn = get_production_connection()
    cur = conn.cursor()

    # Get all schemas
    cur.execute("""
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        ORDER BY schema_name;
    """)
    schemas = cur.fetchall()
    print("\nSchemas:")
    for schema in schemas:
        print(f"  {schema[0]}")

    # Get all tables
    cur.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
          AND table_type = 'BASE TABLE'
        ORDER BY table_schema, table_name;
    """)
    tables = cur.fetchall()
    print("\nAll tables:")
    for schema, table in tables:
        print(f"  {schema}.{table}")

    cur.close()
    conn.close()
    return tables

def explore_invoices_table():
    """Explore the structure of the invoices table in production."""
    print("\n" + "=" * 60)
    print("EXPLORING PRODUCTION INVOICES TABLE")
    print("=" * 60)

    conn = get_production_connection()
    cur = conn.cursor()

    # First find the invoices table in any schema
    cur.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_name LIKE '%invoice%'
        ORDER BY table_schema, table_name;
    """)
    invoice_tables = cur.fetchall()
    print("\nTables containing 'invoice':")
    for schema, table in invoice_tables:
        print(f"  {schema}.{table}")

    if not invoice_tables:
        print("No tables containing 'invoice' found.")
        cur.close()
        conn.close()
        return None

    # Use the first matching table
    schema_name, table_name = invoice_tables[0]
    full_table = f"{schema_name}.{table_name}"

    # Get table structure
    cur.execute(f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
        ORDER BY ordinal_position;
    """)

    columns = cur.fetchall()
    print(f"\nTable Structure for {full_table}:")
    print("-" * 50)
    for col in columns:
        print(f"  {col[0]:<30} {col[1]:<15} {'NULL' if col[2] == 'YES' else 'NOT NULL'}")

    # Get sample data
    print(f"\n\nSample Data from {full_table} (first 5 rows):")
    print("-" * 50)
    cur.execute(f"SELECT * FROM {full_table} LIMIT 5;")
    rows = cur.fetchall()
    col_names = [desc[0] for desc in cur.description]

    for row in rows:
        print("\nRow:")
        for name, val in zip(col_names, row):
            print(f"  {name}: {val}")

    cur.close()
    conn.close()

    return full_table, col_names

def get_production_successful_payments(invoices_table=None):
    """Get successful payments from production invoices table."""
    print("\n" + "=" * 60)
    print("PRODUCTION: SUCCESSFUL PAYMENTS")
    print("=" * 60)

    conn = get_production_connection()
    cur = conn.cursor()

    # If table not provided, find it
    if not invoices_table:
        cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_name LIKE '%invoice%'
            ORDER BY table_schema, table_name;
        """)
        invoice_tables = cur.fetchall()
        if invoice_tables:
            invoices_table = f"{invoice_tables[0][0]}.{invoice_tables[0][1]}"
        else:
            print("No invoices table found!")
            cur.close()
            conn.close()
            return None

    print(f"\nUsing table: {invoices_table}")

    # First, let's see all column names
    schema_name, table_name = invoices_table.split('.')
    cur.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
        ORDER BY ordinal_position;
    """)
    columns = [row[0] for row in cur.fetchall()]
    print(f"Columns: {columns}")

    # Let's see what status values exist
    cur.execute(f"""
        SELECT DISTINCT status, COUNT(*) as cnt
        FROM {invoices_table}
        GROUP BY status
        ORDER BY cnt DESC;
    """)
    statuses = cur.fetchall()
    print("\nStatus distribution:")
    for status, cnt in statuses:
        print(f"  {status}: {cnt}")

    # Determine amount column (could be amount_paid, amount, total, etc.)
    amount_col = None
    for col in ['amount_paid', 'amount', 'total', 'amount_due']:
        if col in columns:
            amount_col = col
            break

    if not amount_col:
        print("Could not find amount column!")
        amount_col = columns[0]  # Fallback

    # Determine date column
    date_col = None
    for col in ['created', 'created_at', 'date', 'invoice_date']:
        if col in columns:
            date_col = col
            break

    print(f"Using amount column: {amount_col}, date column: {date_col}")

    # Get successful payments total - try 'paid' first, then 'succeeded'
    for success_status in ['paid', 'succeeded', 'complete', 'success']:
        cur.execute(f"""
            SELECT COUNT(*) FROM {invoices_table} WHERE status = '{success_status}';
        """)
        if cur.fetchone()[0] > 0:
            break

    cur.execute(f"""
        SELECT
            COUNT(*) as total_count,
            SUM({amount_col}) / 100.0 as total_amount_usd,
            MIN({date_col}) as earliest_date,
            MAX({date_col}) as latest_date
        FROM {invoices_table}
        WHERE status = '{success_status}';
    """)

    result = cur.fetchone()
    print(f"\nSuccessful Payments (status = '{success_status}'):")
    print(f"  Count: {result[0]}")
    print(f"  Total Amount (USD): ${result[1]:,.2f}" if result[1] else "  Total Amount: N/A")
    print(f"  Date Range: {result[2]} to {result[3]}")

    # Get breakdown by month (date column is already timestamp, no conversion needed)
    print("\nMonthly Breakdown:")
    cur.execute(f"""
        SELECT
            DATE_TRUNC('month', {date_col}) as month,
            COUNT(*) as count,
            SUM({amount_col}) / 100.0 as amount_usd
        FROM {invoices_table}
        WHERE status = '{success_status}'
        GROUP BY DATE_TRUNC('month', {date_col})
        ORDER BY month;
    """)
    monthly = cur.fetchall()
    monthly_data = {}
    for month, count, amount in monthly:
        month_str = month.strftime('%Y-%m') if month else 'N/A'
        monthly_data[month_str] = {'count': count, 'amount': float(amount) if amount else 0}
        print(f"  {month_str}: {count} payments, ${amount:,.2f}" if amount else f"  {month}: {count} payments")

    # Get breakdown by day
    print("\nDaily Breakdown:")
    cur.execute(f"""
        SELECT
            DATE_TRUNC('day', {date_col}) as day,
            COUNT(*) as count,
            SUM({amount_col}) / 100.0 as amount_usd
        FROM {invoices_table}
        WHERE status = '{success_status}'
        GROUP BY DATE_TRUNC('day', {date_col})
        ORDER BY day;
    """)
    daily = cur.fetchall()
    daily_data = {}
    for day, count, amount in daily:
        day_str = day.strftime('%Y-%m-%d') if day else 'N/A'
        daily_data[day_str] = {'count': count, 'amount': float(amount) if amount else 0}
        print(f"  {day_str}: {count} payments, ${amount:,.2f}" if amount else f"  {day}: {count} payments")

    cur.close()
    conn.close()

    return {
        'count': result[0],
        'total_usd': float(result[1]) if result[1] else 0,
        'earliest': result[2],
        'latest': result[3],
        'monthly': monthly_data,
        'daily': daily_data
    }

def get_analytics_successful_payments():
    """Get successful payments from analytics mart_stripe_payments."""
    print("\n" + "=" * 60)
    print("ANALYTICS: SUCCESSFUL PAYMENTS (mart_stripe_payments)")
    print("=" * 60)

    conn = get_analytics_connection()
    cur = conn.cursor()

    # Check what tables exist in analytics
    cur.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_name LIKE '%stripe%' OR table_name LIKE '%payment%'
        ORDER BY table_schema, table_name;
    """)
    tables = cur.fetchall()
    print("\nRelevant tables found:")
    for schema, table in tables:
        print(f"  {schema}.{table}")

    # Query mart_stripe_payments for successful payments
    try:
        cur.execute("""
            SELECT
                COUNT(*) as total_count,
                SUM(amount_usd) as total_amount_usd,
                MIN(created_at) as earliest_date,
                MAX(created_at) as latest_date
            FROM analytics.mart_stripe_payments
            WHERE is_successful = true;
        """)

        result = cur.fetchone()
        print(f"\nSuccessful Payments (is_successful = true):")
        print(f"  Count: {result[0]}")
        print(f"  Total Amount (USD): ${result[1]:,.2f}" if result[1] else "  Total Amount: N/A")
        print(f"  Date Range: {result[2]} to {result[3]}")

        # Get monthly breakdown
        print("\nMonthly Breakdown:")
        cur.execute("""
            SELECT
                DATE_TRUNC('month', created_at) as month,
                COUNT(*) as count,
                SUM(amount_usd) as amount_usd
            FROM analytics.mart_stripe_payments
            WHERE is_successful = true
            GROUP BY DATE_TRUNC('month', created_at)
            ORDER BY month;
        """)
        monthly = cur.fetchall()
        monthly_data = {}
        for month, count, amount in monthly:
            month_str = month.strftime('%Y-%m') if month else 'N/A'
            monthly_data[month_str] = {'count': count, 'amount': float(amount) if amount else 0}
            print(f"  {month_str}: {count} payments, ${amount:,.2f}" if amount else f"  {month}: {count} payments")

        # Get daily breakdown
        print("\nDaily Breakdown:")
        cur.execute("""
            SELECT
                DATE_TRUNC('day', created_at) as day,
                COUNT(*) as count,
                SUM(amount_usd) as amount_usd
            FROM analytics.mart_stripe_payments
            WHERE is_successful = true
            GROUP BY DATE_TRUNC('day', created_at)
            ORDER BY day;
        """)
        daily = cur.fetchall()
        daily_data = {}
        for day, count, amount in daily:
            day_str = day.strftime('%Y-%m-%d') if day else 'N/A'
            daily_data[day_str] = {'count': count, 'amount': float(amount) if amount else 0}
            print(f"  {day_str}: {count} payments, ${amount:,.2f}" if amount else f"  {day}: {count} payments")

        cur.close()
        conn.close()

        return {
            'count': result[0],
            'total_usd': float(result[1]) if result[1] else 0,
            'earliest': result[2],
            'latest': result[3],
            'monthly': monthly_data,
            'daily': daily_data
        }
    except Exception as e:
        print(f"\nError querying mart_stripe_payments: {e}")

        # Try to find alternative table
        cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
            ORDER BY table_schema, table_name;
        """)
        all_tables = cur.fetchall()
        print("\nAll tables in database:")
        for schema, table in all_tables:
            print(f"  {schema}.{table}")

        cur.close()
        conn.close()
        return None

def compare_payments(prod, analytics):
    """Compare payment totals between production and analytics."""
    print("\n" + "=" * 60)
    print("COMPARISON RESULTS")
    print("=" * 60)

    if not prod or not analytics:
        print("Cannot compare - missing data from one or both sources.")
        return

    count_diff = prod['count'] - analytics['count']
    amount_diff = prod['total_usd'] - analytics['total_usd']

    print("\n--- OVERALL TOTALS ---")
    print(f"\n{'Metric':<25} {'Production':<20} {'Analytics':<20} {'Difference':<20}")
    print("-" * 85)
    print(f"{'Count':<25} {prod['count']:<20} {analytics['count']:<20} {count_diff:<20}")
    print(f"{'Total USD':<25} ${prod['total_usd']:,.2f}{'':<7} ${analytics['total_usd']:,.2f}{'':<7} ${amount_diff:,.2f}")

    # Date ranges
    print(f"\n{'Date Range':<25}")
    print(f"  Production:  {prod['earliest']} to {prod['latest']}")
    print(f"  Analytics:   {analytics['earliest']} to {analytics['latest']}")

    # Monthly comparison
    print("\n--- MONTHLY COMPARISON ---")
    prod_monthly = prod.get('monthly', {})
    analytics_monthly = analytics.get('monthly', {})
    all_months = sorted(set(prod_monthly.keys()) | set(analytics_monthly.keys()))

    print(f"\n{'Month':<12} {'Prod Count':<12} {'Prod USD':<15} {'Analytics Count':<15} {'Analytics USD':<15} {'Count Diff':<12} {'USD Diff':<15}")
    print("-" * 96)

    for month in all_months:
        p = prod_monthly.get(month, {'count': 0, 'amount': 0})
        a = analytics_monthly.get(month, {'count': 0, 'amount': 0})
        count_d = p['count'] - a['count']
        amount_d = p['amount'] - a['amount']
        print(f"{month:<12} {p['count']:<12} ${p['amount']:>12,.2f}  {a['count']:<15} ${a['amount']:>12,.2f}  {count_d:<12} ${amount_d:>12,.2f}")

    # Daily comparison
    print("\n--- DAILY COMPARISON ---")
    prod_daily = prod.get('daily', {})
    analytics_daily = analytics.get('daily', {})
    all_days = sorted(set(prod_daily.keys()) | set(analytics_daily.keys()))

    print(f"\n{'Date':<12} {'Prod Count':<12} {'Prod USD':<15} {'Analytics Count':<15} {'Analytics USD':<15} {'Count Diff':<12} {'USD Diff':<15}")
    print("-" * 96)

    for day in all_days:
        p = prod_daily.get(day, {'count': 0, 'amount': 0})
        a = analytics_daily.get(day, {'count': 0, 'amount': 0})
        count_d = p['count'] - a['count']
        amount_d = p['amount'] - a['amount']
        # Add marker for days with discrepancy
        marker = " *" if count_d != 0 or abs(amount_d) >= 0.01 else ""
        print(f"{day:<12} {p['count']:<12} ${p['amount']:>12,.2f}  {a['count']:<15} ${a['amount']:>12,.2f}  {count_d:<12} ${amount_d:>12,.2f}{marker}")

    # Calculate percentage difference
    if prod['total_usd'] > 0:
        pct_diff = (amount_diff / prod['total_usd']) * 100
        print(f"\n{'Percentage Difference':<25} {pct_diff:.4f}%")

    print("\n--- VALIDATION STATUS ---")
    if abs(count_diff) == 0 and abs(amount_diff) < 0.01:
        print("[PASSED] Payments match between production and analytics!")
    else:
        print("[DISCREPANCY] Payments do not match!")
        if count_diff != 0:
            print(f"  - Count difference: {count_diff} payments")
        if abs(amount_diff) >= 0.01:
            print(f"  - Amount difference: ${amount_diff:,.2f}")

        # Explain likely causes
        print("\n--- LIKELY EXPLANATION ---")
        if analytics['earliest'] < prod['earliest']:
            print(f"  - Analytics contains older data (from {analytics['earliest']})")
            print(f"  - Production invoices table starts from {prod['earliest']}")
            print("  - The difference likely represents payments recorded before the invoices table was set up")

if __name__ == "__main__":
    print("Payment Validation: Production vs Analytics")
    print("=" * 60)

    # First explore the database structure
    try:
        explore_database_structure()
    except Exception as e:
        print(f"Error exploring database structure: {e}")

    # Explore invoices table structure
    invoices_table = None
    try:
        result = explore_invoices_table()
        if result:
            invoices_table = result[0]
    except Exception as e:
        print(f"Error exploring invoices table: {e}")

    # Get production payments
    try:
        prod_payments = get_production_successful_payments(invoices_table)
    except Exception as e:
        print(f"Error getting production payments: {e}")
        import traceback
        traceback.print_exc()
        prod_payments = None

    # Get analytics payments
    try:
        analytics_payments = get_analytics_successful_payments()
    except Exception as e:
        print(f"Error getting analytics payments: {e}")
        analytics_payments = None

    # Compare
    compare_payments(prod_payments, analytics_payments)
