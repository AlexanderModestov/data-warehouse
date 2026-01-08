#!/bin/bash
# Daily Data Pipeline Orchestration Script
# Runs via Heroku Scheduler at 02:00 UTC
set -e

echo "=== Starting daily data pipeline ==="
echo "Time: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo ""

# ----------------------------------------
# Parse DATABASE_URL for Meltano
# ----------------------------------------
# Heroku provides DATABASE_URL as: postgres://user:password@host:port/dbname
# Meltano needs individual components

if [ -n "$DATABASE_URL" ]; then
  echo "Parsing DATABASE_URL for Meltano..."

  # Remove postgres:// or postgresql:// prefix
  DB_URL_STRIPPED="${DATABASE_URL#*://}"

  # Extract user:password
  DB_CREDENTIALS="${DB_URL_STRIPPED%%@*}"
  export PG_ANALYTICS_USER="${DB_CREDENTIALS%%:*}"
  export PG_ANALYTICS_PASSWORD="${DB_CREDENTIALS#*:}"

  # Extract host:port/dbname
  DB_HOSTPORT_DB="${DB_URL_STRIPPED#*@}"
  DB_HOSTPORT="${DB_HOSTPORT_DB%%/*}"
  export PG_ANALYTICS_HOST="${DB_HOSTPORT%%:*}"
  export PG_ANALYTICS_PORT="${DB_HOSTPORT#*:}"
  export PG_ANALYTICS_DBNAME="${DB_HOSTPORT_DB#*/}"

  echo "  Host: $PG_ANALYTICS_HOST"
  echo "  Port: $PG_ANALYTICS_PORT"
  echo "  Database: $PG_ANALYTICS_DBNAME"

  # Tell Meltano to use Postgres for state storage (not ephemeral SQLite)
  export MELTANO_DATABASE_URI="postgresql://${PG_ANALYTICS_USER}:${PG_ANALYTICS_PASSWORD}@${PG_ANALYTICS_HOST}:${PG_ANALYTICS_PORT}/${PG_ANALYTICS_DBNAME}"
  echo "  Meltano state: Postgres"
  echo ""
fi

# Error handling
notify_failure() {
  echo ""
  echo "PIPELINE_FAILED: $1"
  echo "Time: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
  exit 1
}

trap 'notify_failure "Unexpected error on line $LINENO"' ERR

# ----------------------------------------
# Step 1: Run extractors in parallel
# ----------------------------------------
echo ">>> Step 1: Running data extractors in parallel..."
echo ""

# Run Meltano (Stripe + Amplitude) in background
echo "Starting Meltano extractors..."
(
  cd reluvia

  echo "  Running tap-stripe → raw_stripe..."
  TARGET_SCHEMA=raw_stripe meltano run tap-stripe target-postgres

  echo "  Running tap-amplitude → raw_amplitude..."
  TARGET_SCHEMA=raw_amplitude meltano run tap-amplitude target-postgres

  echo "  Meltano extractors completed"
) &
MELTANO_PID=$!

# Run FunnelFox script in background
echo "Starting FunnelFox extraction..."
(
  python raw_funnelfox.py
  echo "  FunnelFox extraction completed"
) &
FUNNELFOX_PID=$!

# Wait for both to complete
echo ""
echo "Waiting for extractors to finish..."
echo "  Meltano PID: $MELTANO_PID"
echo "  FunnelFox PID: $FUNNELFOX_PID"
echo ""

# Wait for Meltano
wait $MELTANO_PID
MELTANO_EXIT=$?

# Wait for FunnelFox
wait $FUNNELFOX_PID
FUNNELFOX_EXIT=$?

# Check for failures
if [ $MELTANO_EXIT -ne 0 ]; then
  notify_failure "Meltano failed with exit code $MELTANO_EXIT"
fi

if [ $FUNNELFOX_EXIT -ne 0 ]; then
  notify_failure "FunnelFox script failed with exit code $FUNNELFOX_EXIT"
fi

echo ">>> Extractors completed successfully"
echo ""

# ----------------------------------------
# Step 2: Run dbt transformations
# ----------------------------------------
echo ">>> Step 2: Running dbt transformations..."
echo ""

cd dbt

# Run dbt
dbt run --profiles-dir .

echo ""
echo ">>> dbt transformations completed successfully"
echo ""

# ----------------------------------------
# Summary
# ----------------------------------------
echo "=== Pipeline completed successfully ==="
echo "Time: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo ""
echo "Data refreshed:"
echo "  - raw_stripe.* (Stripe charges, subscriptions)"
echo "  - raw_amplitude.* (Product events)"
echo "  - raw_funnelfox.* (Web sessions, subscriptions)"
echo "  - analytics.mart_new_subscriptions"
echo "  - analytics.mart_funnel_conversions"
echo ""
