# Stripe Payments Analytics Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build comprehensive Stripe payment analytics with success/failure tracking, retry detection, and actionable failure intelligence.

**Architecture:** Two dbt models - a detailed fact table (`mart_stripe_payments`) with one row per payment attempt, and a daily summary table (`mart_stripe_payments_daily`) aggregating metrics by day and key dimensions. Retry tracking uses `payment_intent` to link attempts.

**Tech Stack:** dbt, PostgreSQL 17, Stripe data via Meltano

---

## Task 1: Update Source Definitions

**Files:**
- Modify: `dbt/models/sources.yml`

**Step 1: Add missing Stripe charge columns to sources.yml**

Add these columns to the `raw_stripe.charges` table definition:

```yaml
          - name: payment_intent
            description: Payment intent ID (groups retry attempts)
          - name: payment_method_details
            description: JSON containing card details (brand, country, funding)
          - name: billing_details
            description: JSON containing customer billing address
          - name: outcome
            description: JSON containing payment outcome details
```

**Step 2: Commit**

```bash
git add dbt/models/sources.yml
git commit -m "feat: add Stripe charge columns for payment analytics"
```

---

## Task 2: Create Fact Table Schema (Tests First)

**Files:**
- Modify: `dbt/models/marts/schema.yml`

**Step 1: Add mart_stripe_payments model definition with tests**

Add after the `mart_funnel_conversions` definition:

```yaml
  - name: mart_stripe_payments
    description: |
      Stripe payment attempts with success/failure tracking and retry intelligence.
      Grain: One row per payment attempt (charge).

      Use cases:
      - Payment success/failure rate monitoring
      - Failure diagnosis by category and recovery action
      - Retry and recovery rate analysis
      - Revenue and failed revenue tracking by funnel/geo/time
    columns:
      - name: charge_id
        description: Stripe charge ID (primary key)
        tests:
          - unique
          - not_null

      - name: payment_intent_id
        description: Payment intent ID (groups retry attempts together)

      - name: customer_id
        description: Stripe customer ID

      - name: profile_id
        description: FunnelFox master user ID

      - name: status
        description: Payment status (succeeded, failed, pending)
        tests:
          - not_null
          - accepted_values:
              values: ['succeeded', 'failed', 'pending']

      - name: is_successful
        description: Boolean flag for successful payment
        tests:
          - not_null

      - name: amount_usd
        description: Payment amount in USD (converted from cents)
        tests:
          - not_null

      - name: currency
        description: Original currency code

      - name: failure_code
        description: Raw Stripe failure code (null if succeeded)

      - name: failure_category
        description: Categorized failure type
        tests:
          - accepted_values:
              values: ['insufficient_funds', 'card_declined', 'fraud_block', 'authentication_required', 'expired_card', 'invalid_card', 'processing_error', 'technical_error']
              config:
                where: "status = 'failed'"

      - name: recovery_action
        description: Suggested recovery action for failed payments
        tests:
          - accepted_values:
              values: ['retry_eligible', 'request_new_card', 'verify_3ds', 'contact_support']
              config:
                where: "status = 'failed'"

      - name: attempt_number
        description: Attempt number within payment intent (1, 2, 3...)
        tests:
          - not_null

      - name: is_first_attempt
        description: True if this is the first attempt for this payment intent
        tests:
          - not_null

      - name: is_final_attempt
        description: True if this is the last attempt for this payment intent
        tests:
          - not_null

      - name: intent_eventually_succeeded
        description: True if any attempt in this payment intent succeeded
        tests:
          - not_null

      - name: created_at
        description: Payment attempt timestamp (UTC)
        tests:
          - not_null

      - name: created_date
        description: Payment date (UTC)
        tests:
          - not_null

      - name: hour_of_day
        description: Hour of day (0-23)

      - name: day_of_week
        description: Day of week name

      - name: week_start_date
        description: Monday of the week

      - name: month_start_date
        description: First of the month

      - name: funnel_name
        description: Funnel that drove this user

      - name: traffic_source
        description: Traffic source (utm_source or origin)

      - name: traffic_medium
        description: Traffic medium (utm_medium)

      - name: traffic_campaign
        description: Traffic campaign (utm_campaign)

      - name: card_country
        description: Card country of issue

      - name: customer_country
        description: Customer billing country

      - name: card_brand
        description: Card brand (visa, mastercard, amex, etc.)
```

**Step 2: Commit**

```bash
git add dbt/models/marts/schema.yml
git commit -m "feat: add mart_stripe_payments schema with tests"
```

---

## Task 3: Create Fact Table SQL Model

**Files:**
- Create: `dbt/models/marts/mart_stripe_payments.sql`

**Step 1: Create the mart_stripe_payments.sql file**

```sql
{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

/*
    Stripe Payments Mart

    Purpose: Track all payment attempts with success/failure analysis and retry intelligence
    Grain: One row per payment attempt (Stripe charge)

    Business Logic:
    - Revenue calculated as amount / 100.0 (Stripe stores cents)
    - Failure codes categorized into actionable groups
    - Retry attempts linked via payment_intent
    - Recovery tracking shows if failed intents eventually succeeded
*/

WITH stripe_charges AS (
    SELECT
        id AS charge_id,
        payment_intent AS payment_intent_id,
        customer AS customer_id,
        amount / 100.0 AS amount_usd,
        currency,
        status,
        created AS created_at,
        failure_code,
        -- Extract card details from payment_method_details JSON
        payment_method_details::json->>'card'->>'brand' AS card_brand,
        payment_method_details::json->>'card'->>'country' AS card_country,
        -- Extract billing country
        billing_details::json->>'address'->>'country' AS billing_country
    FROM {{ source('raw_stripe', 'charges') }}
),

-- Categorize failures with recovery actions
charges_with_failure_info AS (
    SELECT
        *,
        -- Failure category mapping
        CASE
            WHEN status = 'succeeded' THEN NULL
            WHEN failure_code IN ('insufficient_funds', 'card_velocity_exceeded') THEN 'insufficient_funds'
            WHEN failure_code IN ('card_declined', 'generic_decline', 'do_not_honor') THEN 'card_declined'
            WHEN failure_code IN ('fraudulent', 'merchant_blacklist', 'stolen_card', 'lost_card') THEN 'fraud_block'
            WHEN failure_code IN ('authentication_required', 'card_not_supported') THEN 'authentication_required'
            WHEN failure_code = 'expired_card' THEN 'expired_card'
            WHEN failure_code IN ('invalid_card_number', 'invalid_cvc', 'invalid_expiry_month', 'invalid_expiry_year') THEN 'invalid_card'
            WHEN failure_code IN ('processing_error', 'try_again_later') THEN 'processing_error'
            ELSE 'technical_error'
        END AS failure_category,
        -- Recovery action mapping
        CASE
            WHEN status = 'succeeded' THEN NULL
            WHEN failure_code IN ('insufficient_funds', 'card_velocity_exceeded', 'processing_error', 'try_again_later') THEN 'retry_eligible'
            WHEN failure_code IN ('card_declined', 'generic_decline', 'do_not_honor', 'expired_card', 'invalid_card_number', 'invalid_cvc', 'invalid_expiry_month', 'invalid_expiry_year') THEN 'request_new_card'
            WHEN failure_code IN ('authentication_required', 'card_not_supported') THEN 'verify_3ds'
            ELSE 'contact_support'
        END AS recovery_action
    FROM stripe_charges
),

-- Calculate retry attempt numbers and intent success
intent_stats AS (
    SELECT
        payment_intent_id,
        MAX(CASE WHEN status = 'succeeded' THEN 1 ELSE 0 END) AS intent_has_success
    FROM charges_with_failure_info
    WHERE payment_intent_id IS NOT NULL
    GROUP BY payment_intent_id
),

charges_with_retry_info AS (
    SELECT
        c.*,
        -- Attempt number within payment intent
        ROW_NUMBER() OVER (
            PARTITION BY c.payment_intent_id
            ORDER BY c.created_at ASC
        ) AS attempt_number,
        -- Is first attempt
        CASE
            WHEN ROW_NUMBER() OVER (PARTITION BY c.payment_intent_id ORDER BY c.created_at ASC) = 1 THEN TRUE
            ELSE FALSE
        END AS is_first_attempt,
        -- Is final attempt
        CASE
            WHEN ROW_NUMBER() OVER (PARTITION BY c.payment_intent_id ORDER BY c.created_at DESC) = 1 THEN TRUE
            ELSE FALSE
        END AS is_final_attempt,
        -- Did this intent eventually succeed
        COALESCE(i.intent_has_success = 1, FALSE) AS intent_eventually_succeeded
    FROM charges_with_failure_info c
    LEFT JOIN intent_stats i ON c.payment_intent_id = i.payment_intent_id
),

-- Link to FunnelFox for funnel context
funnelfox_subscriptions AS (
    SELECT
        psp_id,
        profile_id
    FROM {{ source('raw_funnelfox', 'subscriptions') }}
),

funnelfox_sessions AS (
    SELECT
        profile_id,
        funnel_id,
        origin AS traffic_source,
        country AS session_country
    FROM {{ source('raw_funnelfox', 'sessions') }}
),

funnels AS (
    SELECT
        id AS funnel_id,
        title AS funnel_name
    FROM {{ source('raw_funnelfox', 'funnels') }}
),

-- Join to get profile_id and funnel context
charges_with_funnel AS (
    SELECT
        c.*,
        fsub.profile_id,
        sess.funnel_id,
        f.funnel_name,
        sess.traffic_source,
        sess.session_country
    FROM charges_with_retry_info c
    LEFT JOIN funnelfox_subscriptions fsub ON c.charge_id = fsub.psp_id
    LEFT JOIN funnelfox_sessions sess ON fsub.profile_id = sess.profile_id
    LEFT JOIN funnels f ON sess.funnel_id = f.funnel_id
),

-- Final output with all dimensions
final AS (
    SELECT
        -- Identifiers
        charge_id,
        payment_intent_id,
        customer_id,
        profile_id,

        -- Payment outcome
        status,
        CASE WHEN status = 'succeeded' THEN TRUE ELSE FALSE END AS is_successful,
        amount_usd,
        currency,

        -- Failure intelligence
        failure_code,
        failure_category,
        recovery_action,

        -- Retry tracking
        attempt_number,
        is_first_attempt,
        is_final_attempt,
        intent_eventually_succeeded,

        -- Time dimensions
        created_at,
        DATE(created_at AT TIME ZONE 'UTC') AS created_date,
        EXTRACT(HOUR FROM created_at AT TIME ZONE 'UTC')::INT AS hour_of_day,
        TO_CHAR(created_at AT TIME ZONE 'UTC', 'Day') AS day_of_week,
        DATE_TRUNC('week', created_at AT TIME ZONE 'UTC')::DATE AS week_start_date,
        DATE_TRUNC('month', created_at AT TIME ZONE 'UTC')::DATE AS month_start_date,

        -- Funnel dimensions
        funnel_name,
        traffic_source,
        NULL AS traffic_medium,  -- Not available in current data
        NULL AS traffic_campaign,  -- Not available in current data

        -- Geographic dimensions
        card_country,
        COALESCE(billing_country, session_country) AS customer_country,

        -- Card details
        card_brand

    FROM charges_with_funnel
)

SELECT * FROM final
```

**Step 2: Commit**

```bash
git add dbt/models/marts/mart_stripe_payments.sql
git commit -m "feat: add mart_stripe_payments fact table model"
```

---

## Task 4: Create Daily Summary Table Schema

**Files:**
- Modify: `dbt/models/marts/schema.yml`

**Step 1: Add mart_stripe_payments_daily model definition**

Add after `mart_stripe_payments`:

```yaml
  - name: mart_stripe_payments_daily
    description: |
      Daily aggregated Stripe payment metrics.
      Grain: One row per day + funnel + card_country + card_brand.

      Use cases:
      - Daily payment dashboard
      - Success rate trends
      - Failure category breakdown
      - Recovery rate monitoring
    columns:
      - name: date
        description: The date
        tests:
          - not_null

      - name: funnel_name
        description: Funnel name (null for all funnels)

      - name: card_country
        description: Card country (null for all countries)

      - name: card_brand
        description: Card brand (null for all brands)

      - name: total_attempts
        description: Total payment attempts
        tests:
          - not_null

      - name: successful_payments
        description: Count of successful payments
        tests:
          - not_null

      - name: failed_payments
        description: Count of failed payments
        tests:
          - not_null

      - name: success_rate
        description: Success rate (0.0 - 1.0)

      - name: gross_revenue_usd
        description: Sum of successful payment amounts

      - name: failed_revenue_usd
        description: Sum of failed payment amounts (potential lost revenue)

      - name: failures_insufficient_funds
        description: Count of insufficient funds failures

      - name: failures_card_declined
        description: Count of card declined failures

      - name: failures_fraud_block
        description: Count of fraud block failures

      - name: failures_authentication_required
        description: Count of authentication required failures

      - name: failures_technical_error
        description: Count of technical error failures

      - name: failures_other
        description: Count of other failures

      - name: intents_with_retry
        description: Payment intents that had multiple attempts

      - name: intents_recovered
        description: Failed intents that eventually succeeded

      - name: recovery_rate
        description: Recovery rate (recovered / intents_with_retry)
```

**Step 2: Commit**

```bash
git add dbt/models/marts/schema.yml
git commit -m "feat: add mart_stripe_payments_daily schema"
```

---

## Task 5: Create Daily Summary Table SQL Model

**Files:**
- Create: `dbt/models/marts/mart_stripe_payments_daily.sql`

**Step 1: Create the mart_stripe_payments_daily.sql file**

```sql
{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

/*
    Stripe Payments Daily Mart

    Purpose: Daily aggregated payment metrics for dashboards and trend analysis
    Grain: One row per day + funnel + card_country + card_brand

    Built on top of mart_stripe_payments for consistent business logic
*/

WITH payments AS (
    SELECT * FROM {{ ref('mart_stripe_payments') }}
),

-- Aggregate by date and dimensions
daily_metrics AS (
    SELECT
        created_date AS date,
        funnel_name,
        card_country,
        card_brand,

        -- Volume metrics
        COUNT(*) AS total_attempts,
        SUM(CASE WHEN is_successful THEN 1 ELSE 0 END) AS successful_payments,
        SUM(CASE WHEN NOT is_successful THEN 1 ELSE 0 END) AS failed_payments,

        -- Revenue metrics
        SUM(CASE WHEN is_successful THEN amount_usd ELSE 0 END) AS gross_revenue_usd,
        SUM(CASE WHEN NOT is_successful THEN amount_usd ELSE 0 END) AS failed_revenue_usd,

        -- Failure breakdown
        SUM(CASE WHEN failure_category = 'insufficient_funds' THEN 1 ELSE 0 END) AS failures_insufficient_funds,
        SUM(CASE WHEN failure_category = 'card_declined' THEN 1 ELSE 0 END) AS failures_card_declined,
        SUM(CASE WHEN failure_category = 'fraud_block' THEN 1 ELSE 0 END) AS failures_fraud_block,
        SUM(CASE WHEN failure_category = 'authentication_required' THEN 1 ELSE 0 END) AS failures_authentication_required,
        SUM(CASE WHEN failure_category IN ('expired_card', 'invalid_card', 'processing_error', 'technical_error') THEN 1 ELSE 0 END) AS failures_technical_error,
        SUM(CASE WHEN failure_category IS NOT NULL AND failure_category NOT IN ('insufficient_funds', 'card_declined', 'fraud_block', 'authentication_required', 'expired_card', 'invalid_card', 'processing_error', 'technical_error') THEN 1 ELSE 0 END) AS failures_other

    FROM payments
    GROUP BY
        created_date,
        funnel_name,
        card_country,
        card_brand
),

-- Calculate retry/recovery metrics (need to aggregate at intent level first)
intent_metrics AS (
    SELECT
        created_date,
        funnel_name,
        card_country,
        card_brand,
        payment_intent_id,
        COUNT(*) AS attempts_in_intent,
        MAX(CASE WHEN is_successful THEN 1 ELSE 0 END) AS intent_succeeded,
        MIN(CASE WHEN NOT is_successful THEN 1 ELSE 0 END) AS intent_had_failure
    FROM payments
    WHERE payment_intent_id IS NOT NULL
    GROUP BY
        created_date,
        funnel_name,
        card_country,
        card_brand,
        payment_intent_id
),

recovery_metrics AS (
    SELECT
        created_date AS date,
        funnel_name,
        card_country,
        card_brand,
        -- Intents with multiple attempts (had at least one retry)
        SUM(CASE WHEN attempts_in_intent > 1 THEN 1 ELSE 0 END) AS intents_with_retry,
        -- Intents that had a failure but eventually succeeded
        SUM(CASE WHEN intent_had_failure = 1 AND intent_succeeded = 1 THEN 1 ELSE 0 END) AS intents_recovered
    FROM intent_metrics
    GROUP BY
        created_date,
        funnel_name,
        card_country,
        card_brand
),

-- Combine all metrics
final AS (
    SELECT
        dm.date,
        dm.funnel_name,
        dm.card_country,
        dm.card_brand,

        -- Volume
        dm.total_attempts,
        dm.successful_payments,
        dm.failed_payments,
        CASE
            WHEN dm.total_attempts > 0 THEN dm.successful_payments::NUMERIC / dm.total_attempts
            ELSE NULL
        END AS success_rate,

        -- Revenue
        dm.gross_revenue_usd,
        dm.failed_revenue_usd,

        -- Failure breakdown
        dm.failures_insufficient_funds,
        dm.failures_card_declined,
        dm.failures_fraud_block,
        dm.failures_authentication_required,
        dm.failures_technical_error,
        dm.failures_other,

        -- Recovery
        COALESCE(rm.intents_with_retry, 0) AS intents_with_retry,
        COALESCE(rm.intents_recovered, 0) AS intents_recovered,
        CASE
            WHEN COALESCE(rm.intents_with_retry, 0) > 0 THEN rm.intents_recovered::NUMERIC / rm.intents_with_retry
            ELSE NULL
        END AS recovery_rate

    FROM daily_metrics dm
    LEFT JOIN recovery_metrics rm
        ON dm.date = rm.date
        AND COALESCE(dm.funnel_name, '') = COALESCE(rm.funnel_name, '')
        AND COALESCE(dm.card_country, '') = COALESCE(rm.card_country, '')
        AND COALESCE(dm.card_brand, '') = COALESCE(rm.card_brand, '')
)

SELECT * FROM final
```

**Step 2: Commit**

```bash
git add dbt/models/marts/mart_stripe_payments_daily.sql
git commit -m "feat: add mart_stripe_payments_daily summary model"
```

---

## Task 6: Validate Models with dbt

**Step 1: Run dbt compile to check SQL syntax**

```bash
cd dbt && dbt compile --select mart_stripe_payments mart_stripe_payments_daily
```

Expected: Compilation succeeds with no errors

**Step 2: Run dbt run to build models (requires database connection)**

```bash
cd dbt && dbt run --select mart_stripe_payments mart_stripe_payments_daily
```

Expected: Models build successfully

**Step 3: Run dbt test to validate schema tests**

```bash
cd dbt && dbt test --select mart_stripe_payments mart_stripe_payments_daily
```

Expected: All tests pass

**Step 4: Commit any fixes if needed**

```bash
git add -A
git commit -m "fix: address dbt validation issues"
```

---

## Task 7: Final Review and Merge Preparation

**Step 1: Review all changes**

```bash
git log --oneline feature/stripe-payments-analytics ^main
```

**Step 2: Ensure all tests pass**

```bash
cd dbt && dbt test --select mart_stripe_payments mart_stripe_payments_daily
```

**Step 3: Ready for merge**

Use `superpowers:finishing-a-development-branch` to complete the work.

---

## Summary

| Task | Description | Files |
|------|-------------|-------|
| 1 | Update source definitions | sources.yml |
| 2 | Create fact table schema | schema.yml |
| 3 | Create fact table SQL | mart_stripe_payments.sql |
| 4 | Create summary table schema | schema.yml |
| 5 | Create summary table SQL | mart_stripe_payments_daily.sql |
| 6 | Validate with dbt | - |
| 7 | Final review | - |
