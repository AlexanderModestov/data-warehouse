# dbt Quick Start Guide

## 1. Install dbt

```bash
pip install dbt-postgres
```

## 2. Set Up Profile

Your environment variables are already configured in `.env`. Verify they're set:

```bash
echo $PG_ANALYTICS_HOST
echo $PG_ANALYTICS_USER
echo $PG_ANALYTICS_DBNAME
```

Copy the example profile to your home directory:

```bash
mkdir -p ~/.dbt
cp profiles.yml.example ~/.dbt/profiles.yml
```

Or dbt will automatically use your environment variables.

## 3. Test Connection

```bash
cd dbt
dbt debug
```

You should see:
```
All checks passed!
```

## 4. Run Your First Models

```bash
# Run all marts
dbt run

# You should see:
# Completed successfully
# Done. PASS=2 ...
```

This creates two tables in your `analytics` schema:
- `analytics.mart_new_subscriptions`
- `analytics.mart_funnel_conversions`

## 5. Run Tests

```bash
dbt test
```

This validates:
- Unique subscription IDs
- Not null constraints
- Accepted values for conversion flags

## 6. View Documentation

```bash
dbt docs generate
dbt docs serve
```

Opens a web interface at http://localhost:8080 showing your models, lineage, and column descriptions.

## 7. Query Your Data

Connect to your PostgreSQL database and query:

```sql
-- New subscriptions today
SELECT
    subscription_date,
    COUNT(*) AS new_subs,
    SUM(revenue_usd) AS total_revenue
FROM analytics.mart_new_subscriptions
WHERE subscription_date = CURRENT_DATE
GROUP BY subscription_date;

-- Conversion rate by funnel
SELECT
    funnel_title,
    COUNT(*) AS total_sessions,
    SUM(converted) AS conversions,
    ROUND(100.0 * SUM(converted) / COUNT(*), 2) AS conversion_rate_pct
FROM analytics.mart_funnel_conversions
GROUP BY funnel_title
ORDER BY conversion_rate_pct DESC;
```

## Scheduling

To refresh data daily, add to your cron or use Meltano:

```bash
# Daily at 2 AM
0 2 * * * cd /path/to/DWH/dbt && dbt run
```

## Troubleshooting

**"Profile not found"**
- Check `~/.dbt/profiles.yml` exists
- Or ensure environment variables are set

**"Schema does not exist"**
- Run your Meltano pipelines first to populate raw data
- Check schema names match in sources.yml

**"Relation does not exist"**
- Verify table names in sources.yml match your actual database tables
- Run `\dt raw_funnelfox.*` in psql to see actual table names
