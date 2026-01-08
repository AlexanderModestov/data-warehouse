# dbt Analytics Marts Design

**Date:** 2026-01-06
**Purpose:** Build analytics layer for daily executive reporting and deep-dive analytics

## Overview

Single-layer dbt architecture with mart models that join directly from raw schemas to produce analytics-ready tables for:
- New subscriptions tracking
- Funnel-to-purchase conversion analysis

## Architecture

### Project Structure

```
DWH/
├── dbt/
│   ├── dbt_project.yml
│   ├── packages.yml (optional)
│   └── models/
│       ├── sources.yml
│       └── marts/
│           ├── mart_new_subscriptions.sql
│           └── mart_funnel_conversions.sql
```

### Design Principles

- **No staging layer:** Raw schemas (`raw_funnelfox`, `raw_stripe`, `raw_amplitude`, `raw_facebook`) serve as staging
- **Direct joins in marts:** Each mart performs its own joins from raw sources
- **Table materialization:** All marts materialized as tables (not views) for dashboard performance
- **Daily refresh:** Run via scheduled jobs, no incremental models needed

## Data Sources

### Schema Mapping

- `raw_funnelfox` - Web funnel sessions and subscriptions
- `raw_stripe` - Payment charges and subscription records
- `raw_amplitude` - Product events from Amplitude
- `raw_facebook` - Ad performance and campaign data

### Identity Linking Rules

**Primary Identity Chain:**
1. **Master ID:** `raw_funnelfox.sessions.profile_id` (the glue)
2. **Stripe Link:** `raw_stripe.charges.id` ↔ `raw_funnelfox.subscriptions.psp_id`
3. **Amplitude Link:** `raw_amplitude.events.user_id` ↔ `profile_id`
4. **Fallback:** Use `email` or `device_id` when `user_id` is null

**Join Pattern:**
```sql
-- Link funnelfox sessions to stripe charges
LEFT JOIN raw_funnelfox.subscriptions sub
  ON sub.profile_id = sessions.profile_id
LEFT JOIN raw_stripe.charges chg
  ON chg.id = sub.psp_id
  AND chg.status = 'succeeded'

-- Link to amplitude events
LEFT JOIN raw_amplitude.events evt
  ON evt.user_id = sessions.profile_id
  OR (evt.user_id IS NULL AND evt.email = sessions.email)
```

## Mart Models

### mart_new_subscriptions.sql

**Purpose:** Track new subscriptions with revenue and acquisition context

**Data Sources:**
- `raw_stripe.charges` (revenue data)
- `raw_stripe.subscriptions` (subscription details)
- `raw_funnelfox.subscriptions` (links to funnelfox via psp_id)
- `raw_funnelfox.sessions` (acquisition context via profile_id)
- `raw_facebook` (optional: ad attribution)

**Business Logic:**
- Join `raw_stripe.charges.id` = `raw_funnelfox.subscriptions.psp_id`
- Calculate revenue: `amount / 100.0 AS revenue_usd` (Stripe stores cents as integers)
- Filter successful: `status = 'succeeded'`
- Get first charge per subscription for "new" logic
- Pull funnel_id, country, origin from sessions via profile_id
- Exclude sandbox: `raw_funnelfox.subscriptions.sandbox = false`

**Output Grain:** One row per new subscription

**Columns:**
- subscription_date
- subscription_id
- user_profile_id
- revenue_usd
- currency
- funnel_id
- funnel_title
- country
- city
- payment_provider
- is_sandbox

### mart_funnel_conversions.sql

**Purpose:** Calculate web-to-subscription conversion rates and time-to-convert

**Data Sources:**
- `raw_funnelfox.sessions` (funnel entry point)
- `raw_stripe.charges` (conversion event)
- `raw_amplitude.events` (optional: app onboarding steps)

**Business Logic:**
- Start with all sessions (potential conversions)
- Left join to charges via profile_id → psp_id path
- Calculate `converted = CASE WHEN charge_id IS NOT NULL THEN 1 ELSE 0 END`
- Calculate `time_to_conversion = charge_created_at - session_created_at`
- Group by date/funnel for aggregation

**Output Grain:** One row per session (converted or not)

**Columns:**
- session_date
- session_id
- profile_id
- funnel_id
- funnel_title
- country
- converted (0/1 flag)
- revenue_usd (null if not converted)
- time_to_conversion_hours
- had_purchase (boolean)

## Configuration

### dbt_project.yml

```yaml
name: 'reluvia_analytics'
version: '1.0.0'
config-version: 2

profile: 'reluvia'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  reluvia_analytics:
    marts:
      +materialized: table
      +schema: analytics
```

### profiles.yml

Located in `~/.dbt/` or configured via environment variables:

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

### sources.yml

```yaml
version: 2

sources:
  - name: raw_funnelfox
    schema: raw_funnelfox
    tables:
      - name: sessions
        description: Web funnel sessions
      - name: subscriptions
        description: FunnelFox subscription records
      - name: funnels
        description: Funnel configurations

  - name: raw_stripe
    schema: raw_stripe
    tables:
      - name: charges
        description: Stripe payment charges
      - name: subscriptions
        description: Stripe subscription records

  - name: raw_amplitude
    schema: raw_amplitude
    tables:
      - name: events
        description: Product events from Amplitude

  - name: raw_facebook
    schema: raw_facebook
    tables:
      - name: ad_statistics
        description: Facebook ad performance metrics
      - name: campaigns
        description: Facebook campaign details
```

## Business Logic Rules

Per CLAUDE.md:

- **Revenue Calculation:** `amount / 100.0` (Stripe stores amounts as integers in cents)
- **Successful Sale:** `raw_stripe.charges.status = 'succeeded'`
- **Timezone:** All analysis in UTC
- **Sandbox Exclusion:** Filter out `sandbox = true` transactions
- **Transaction Health:** Monitor `failure_code` in charges for checkout friction

## Data Quality & Testing

**Recommended Tests:**
- Source freshness checks on raw tables
- Uniqueness tests on mart primary keys (subscription_id, session_id)
- Not null tests on critical fields (profile_id, revenue_usd, dates)
- Referential integrity between profile_id and related records

## Usage Patterns

**For Executives (Daily Reporting):**
- Query `analytics.mart_new_subscriptions` aggregated by date
- Query `analytics.mart_funnel_conversions` for conversion rates by funnel

**For Analysts (Deep Dives):**
- Session-level detail in `analytics.mart_funnel_conversions`
- Subscription-level detail in `analytics.mart_new_subscriptions`
- Join back to raw tables for custom analysis

## Next Steps for Implementation

1. Initialize dbt project in `DWH/dbt/`
2. Create configuration files (dbt_project.yml, sources.yml)
3. Build mart models with SQL
4. Test locally with `dbt run`
5. Add data quality tests
6. Schedule daily runs via Meltano or external orchestrator
