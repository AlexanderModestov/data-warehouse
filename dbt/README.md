# Reluvia Analytics - dbt Project

Analytics layer for Reluvia data warehouse, transforming raw data into business-ready marts.

## Project Structure

```
dbt/
├── dbt_project.yml          # dbt project configuration
├── models/
│   ├── sources.yml          # Raw data source definitions
│   └── marts/               # Business-ready analytics tables
│       ├── schema.yml       # Model documentation and tests
│       ├── mart_new_subscriptions.sql
│       └── mart_funnel_conversions.sql
```

## Setup

### 1. Install dbt

```bash
pip install dbt-postgres
```

### 2. Configure Profile

Create or edit `~/.dbt/profiles.yml`:

```yaml
reluvia:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('PG_ANALYTICS_HOST') }}"
      user: "{{ env_var('PG_ANALYTICS_USER') }}"
      password: "{{ env_var('PG_ANALYTICS_PASSWORD') }}"
      port: "{{ env_var('PG_ANALYTICS_PORT') }}"
      dbname: "{{ env_var('PG_ANALYTICS_DBNAME') }}"
      schema: analytics
      threads: 4
```

Ensure environment variables are set in your `.env` file.

### 3. Test Connection

```bash
cd dbt
dbt debug
```

## Running Models

### Run all models
```bash
dbt run
```

### Run specific model
```bash
dbt run --select mart_new_subscriptions
dbt run --select mart_funnel_conversions
```

### Run with full refresh
```bash
dbt run --full-refresh
```

## Testing

### Run all tests
```bash
dbt test
```

### Test specific model
```bash
dbt test --select mart_new_subscriptions
```

## Documentation

### Generate documentation
```bash
dbt docs generate
```

### Serve documentation site
```bash
dbt docs serve
```

## Data Models

### mart_new_subscriptions

**Purpose:** Track new subscriptions with revenue and acquisition context

**Grain:** One row per new subscription (first successful charge)

**Key Metrics:**
- Revenue (USD)
- Time to convert
- Geographic breakdown
- Funnel performance

**Use Cases:**
- Daily subscription revenue reporting
- Funnel ROI analysis
- Geographic revenue analysis

### mart_funnel_conversions

**Purpose:** Calculate web-to-subscription conversion rates

**Grain:** One row per funnel session (converted or not)

**Key Metrics:**
- Conversion rate by funnel
- Time to conversion distribution
- Traffic source performance

**Use Cases:**
- Conversion funnel optimization
- A/B test analysis
- Traffic source ROI

## Data Sources

All models read from raw schemas populated by Meltano:

- `raw_funnelfox` - Web funnel data
- `raw_stripe` - Payment and subscription data
- `raw_amplitude` - Product event data
- `raw_facebook` - Ad performance data

## Scheduling

To run dbt transformations daily:

### Option 1: Meltano Integration
Add to `meltano.yml` and schedule with `meltano schedule`

### Option 2: Cron Job
```bash
0 2 * * * cd /path/to/DWH/dbt && dbt run
```

### Option 3: Airflow/Dagster
Create DAG with dbt operators

## Business Logic

Per `CLAUDE.md` rules:

- **Revenue:** `amount / 100.0` (Stripe stores cents)
- **Successful charges:** `status = 'succeeded'`
- **Timezone:** All timestamps in UTC
- **Sandbox exclusion:** `sandbox = FALSE`
- **Identity linking:** `profile_id` is master ID

## Troubleshooting

### Connection issues
```bash
dbt debug
```

### Schema doesn't exist
Ensure Meltano has run and populated raw schemas

### Slow queries
Check indexes on raw tables (especially `created_at`, `profile_id`, `psp_id`)
