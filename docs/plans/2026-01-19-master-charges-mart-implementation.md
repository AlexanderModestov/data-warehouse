# Master Charges Mart Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Create `mart_master_charges` - a single source of truth for all Stripe payment analytics that consolidates payment, subscription, and risk metrics.

**Architecture:** Charge-centric fact table (one row per Stripe charge) with denormalized risk/refund fields and optional FunnelFox linkage. Uses LEFT JOINs from charges to preserve all payments, with FunnelFox fields nullable for unlinked charges.

**Tech Stack:** dbt (SQL), PostgreSQL, Jinja templating

---

## Task 1: Create Base Model Structure

**Files:**
- Create: `dbt/models/marts/mart_master_charges.sql`

**Step 1: Create the model file with config and header comment**

```sql
{{
    config(
        materialized='table'
    )
}}

/*
    Master Charges Mart

    Purpose: Single source of truth for all Stripe payment analytics
    Grain: One row per Stripe charge (charge_id)

    Data Sources:
    - raw_stripe.charges: Core payment data
    - raw_stripe.refunds: Refund details
    - raw_funnelfox.subscriptions: FunnelFox linkage (nullable)
    - raw_funnelfox.sessions: User session data (nullable)
    - raw_funnelfox.funnels: Funnel metadata (nullable)

    Business Logic:
    - Revenue in USD (amount / 100.0)
    - Exclude test payments ($1, $2)
    - FunnelFox linkage via subscriptions.psp_id = charges.id
    - Risk metrics from outcome JSON (EFW proxy approach)
    - Refund aggregates + latest refund details from refunds table
*/

-- Placeholder for CTEs
SELECT 1
```

**Step 2: Verify file created**

Run: `dir dbt\models\marts\mart_master_charges.sql`
Expected: File exists

---

## Task 2: Build Core Charges CTE

**Files:**
- Modify: `dbt/models/marts/mart_master_charges.sql`

**Step 1: Replace placeholder with stripe_charges CTE**

Replace `SELECT 1` with:

```sql
WITH stripe_charges AS (
    SELECT
        -- Core identifiers
        id AS charge_id,
        payment_intent AS payment_intent_id,
        customer AS customer_id,
        invoice AS invoice_id,

        -- Amounts
        amount / 100.0 AS amount_usd,
        currency,

        -- Status
        status,
        status = 'succeeded' AS is_successful,
        failure_code,

        -- Card info from payment_method_details JSON
        LOWER((payment_method_details::json->'card')->>'brand') AS card_brand,
        (payment_method_details::json->'card')->>'country' AS card_country,
        (payment_method_details::json->'card')->>'funding' AS card_funding,
        (payment_method_details::json->'card')->>'last4' AS card_last4,

        -- Customer billing country
        (billing_details::json->'address')->>'country' AS customer_country,

        -- Risk data from outcome JSON
        (outcome::json)->>'risk_level' AS risk_level,
        ((outcome::json)->>'risk_score')::INT AS risk_score,
        (outcome::json)->>'type' AS outcome_type,
        (outcome::json)->>'reason' AS outcome_reason,

        -- Dispute flag (embedded on charge)
        COALESCE(disputed, FALSE) AS is_disputed,
        dispute AS dispute_id,

        -- Embedded refund data (for fallback)
        COALESCE(refunded, FALSE) AS has_refund_embedded,
        COALESCE(amount_refunded, 0) / 100.0 AS refund_amount_embedded,

        -- Timestamps
        created AS created_at

    FROM {{ source('raw_stripe', 'charges') }}
    WHERE amount NOT IN (100, 200)  -- Exclude test payments ($1, $2)
)

SELECT * FROM stripe_charges
LIMIT 10
```

**Step 2: Test the CTE compiles**

Run: `cd dbt && dbt compile --select mart_master_charges`
Expected: Compilation successful

---

## Task 3: Add Refund Details CTE

**Files:**
- Modify: `dbt/models/marts/mart_master_charges.sql`

**Step 1: Add refund_details CTE after stripe_charges**

Insert before final SELECT:

```sql
-- Aggregate refund data per charge with latest refund details
refund_details AS (
    SELECT
        charge AS charge_id,
        COUNT(*) AS refund_count,
        SUM(amount) / 100.0 AS total_refund_amount_usd,
        MAX(created) AS latest_refund_date,
        -- Get latest refund reason using ROW_NUMBER
        (
            SELECT reason
            FROM {{ source('raw_stripe', 'refunds') }} r2
            WHERE r2.charge = r.charge
            ORDER BY r2.created DESC
            LIMIT 1
        ) AS latest_refund_reason
    FROM {{ source('raw_stripe', 'refunds') }} r
    WHERE status = 'succeeded'
    GROUP BY charge
),
```

**Step 2: Test compilation**

Run: `cd dbt && dbt compile --select mart_master_charges`
Expected: Compilation successful

---

## Task 4: Add FunnelFox Subscription Linkage CTE

**Files:**
- Modify: `dbt/models/marts/mart_master_charges.sql`

**Step 1: Add funnelfox_subscriptions CTE**

Insert after refund_details:

```sql
-- FunnelFox subscriptions for linkage
funnelfox_subscriptions AS (
    SELECT
        psp_id,  -- Links to charge_id
        id AS ff_subscription_id,
        profile_id,
        status AS ff_subscription_status,
        payment_provider,
        billing_interval,
        billing_interval_count,
        price / 100.0 AS subscription_price_usd
    FROM {{ source('raw_funnelfox', 'subscriptions') }}
    WHERE sandbox = false
),
```

**Step 2: Test compilation**

Run: `cd dbt && dbt compile --select mart_master_charges`
Expected: Compilation successful

---

## Task 5: Add First Session CTE

**Files:**
- Modify: `dbt/models/marts/mart_master_charges.sql`

**Step 1: Add first_sessions CTE**

Insert after funnelfox_subscriptions:

```sql
-- First session per user for attribution
first_sessions AS (
    SELECT DISTINCT ON (profile_id)
        profile_id,
        id AS session_id,
        funnel_id,
        country,
        city,
        origin AS traffic_source,
        created_at AS first_session_at
    FROM {{ source('raw_funnelfox', 'sessions') }}
    ORDER BY profile_id, created_at ASC
),
```

**Step 2: Test compilation**

Run: `cd dbt && dbt compile --select mart_master_charges`
Expected: Compilation successful

---

## Task 6: Add Funnels Metadata CTE

**Files:**
- Modify: `dbt/models/marts/mart_master_charges.sql`

**Step 1: Add funnels CTE**

Insert after first_sessions:

```sql
-- Funnel metadata
funnels AS (
    SELECT
        id AS funnel_id,
        title AS funnel_title,
        type AS funnel_type,
        environment AS funnel_environment
    FROM {{ source('raw_funnelfox', 'funnels') }}
),
```

**Step 2: Test compilation**

Run: `cd dbt && dbt compile --select mart_master_charges`
Expected: Compilation successful

---

## Task 7: Add Failure Categorization CTE

**Files:**
- Modify: `dbt/models/marts/mart_master_charges.sql`

**Step 1: Add charges_with_categories CTE**

Insert after funnels:

```sql
-- Add failure categorization and derived risk flags
charges_with_categories AS (
    SELECT
        c.*,

        -- Failure categorization
        CASE
            WHEN c.status = 'succeeded' THEN NULL
            WHEN c.failure_code IN ('insufficient_funds', 'card_velocity_exceeded') THEN 'insufficient_funds'
            WHEN c.failure_code IN ('card_declined', 'generic_decline', 'do_not_honor') THEN 'card_declined'
            WHEN c.failure_code IN ('fraudulent', 'merchant_blacklist', 'stolen_card', 'lost_card') THEN 'fraud_block'
            WHEN c.failure_code IN ('authentication_required', 'card_not_supported') THEN 'authentication_required'
            WHEN c.failure_code = 'expired_card' THEN 'expired_card'
            WHEN c.failure_code IN ('invalid_card_number', 'invalid_cvc', 'invalid_expiry_month', 'invalid_expiry_year') THEN 'invalid_card'
            WHEN c.failure_code IN ('processing_error', 'try_again_later') THEN 'processing_error'
            ELSE 'technical_error'
        END AS failure_category,

        -- Recovery action
        CASE
            WHEN c.status = 'succeeded' THEN NULL
            WHEN c.failure_code IN ('insufficient_funds', 'card_velocity_exceeded', 'processing_error', 'try_again_later') THEN 'retry_eligible'
            WHEN c.failure_code IN ('card_declined', 'generic_decline', 'do_not_honor', 'expired_card', 'invalid_card_number', 'invalid_cvc', 'invalid_expiry_month', 'invalid_expiry_year') THEN 'request_new_card'
            WHEN c.failure_code IN ('authentication_required', 'card_not_supported') THEN 'verify_3ds'
            ELSE 'contact_support'
        END AS recovery_action,

        -- Derived risk flags (EFW proxy approach)
        c.outcome_type = 'blocked' AS is_blocked,
        c.failure_code IN ('fraudulent', 'merchant_blacklist', 'stolen_card', 'lost_card') AS is_fraud_decline

    FROM stripe_charges c
),
```

**Step 2: Test compilation**

Run: `cd dbt && dbt compile --select mart_master_charges`
Expected: Compilation successful

---

## Task 8: Build Final SELECT with All Joins

**Files:**
- Modify: `dbt/models/marts/mart_master_charges.sql`

**Step 1: Replace final SELECT with complete output**

```sql
-- Final output: join all sources
SELECT
    -- Core charge fields
    c.charge_id,
    c.payment_intent_id,
    c.customer_id,
    c.invoice_id,
    c.amount_usd,
    c.currency,
    c.status,
    c.is_successful,
    c.failure_code,
    c.failure_category,
    c.recovery_action,

    -- Card info
    c.card_brand,
    c.card_country,
    c.card_funding,
    c.card_last4,
    c.customer_country,

    -- Risk fields
    c.risk_level,
    c.risk_score,
    c.outcome_type,
    c.outcome_reason,
    c.is_blocked,
    c.is_fraud_decline,

    -- Dispute fields
    c.is_disputed,
    c.dispute_id,

    -- Refund fields (prefer refunds table, fallback to embedded)
    COALESCE(ref.refund_count, 0) AS refund_count,
    COALESCE(ref.total_refund_amount_usd, c.refund_amount_embedded) AS refund_amount_usd,
    COALESCE(ref.total_refund_amount_usd, c.refund_amount_embedded) >= c.amount_usd AS is_fully_refunded,
    CASE
        WHEN c.amount_usd > 0
        THEN COALESCE(ref.total_refund_amount_usd, c.refund_amount_embedded) / c.amount_usd
        ELSE 0
    END AS refund_ratio,
    ref.latest_refund_date,
    ref.latest_refund_reason,

    -- FunnelFox linkage (nullable)
    ff.profile_id,
    ff.ff_subscription_id,
    ff.ff_subscription_status,
    ff.payment_provider,
    ff.billing_interval,
    ff.billing_interval_count,
    ff.subscription_price_usd,

    -- Funnel & traffic info (nullable)
    f.funnel_id,
    f.funnel_title,
    f.funnel_type,
    f.funnel_environment,
    sess.country,
    sess.city,
    sess.traffic_source,
    sess.first_session_at,

    -- Time dimensions
    c.created_at,
    DATE(c.created_at AT TIME ZONE 'UTC') AS created_date,
    EXTRACT(HOUR FROM c.created_at AT TIME ZONE 'UTC')::INT AS hour_of_day,
    TRIM(TO_CHAR(c.created_at AT TIME ZONE 'UTC', 'Day')) AS day_of_week,
    DATE_TRUNC('week', c.created_at AT TIME ZONE 'UTC')::DATE AS week_start_date,
    DATE_TRUNC('month', c.created_at AT TIME ZONE 'UTC')::DATE AS month_start_date,

    -- Organic flag (no FunnelFox linkage)
    ff.ff_subscription_id IS NULL AS is_organic

FROM charges_with_categories c

-- Refund details
LEFT JOIN refund_details ref
    ON ref.charge_id = c.charge_id

-- FunnelFox subscription linkage
LEFT JOIN funnelfox_subscriptions ff
    ON ff.psp_id = c.charge_id

-- First session for attribution
LEFT JOIN first_sessions sess
    ON sess.profile_id = ff.profile_id

-- Funnel metadata
LEFT JOIN funnels f
    ON f.funnel_id = sess.funnel_id
```

**Step 2: Test compilation**

Run: `cd dbt && dbt compile --select mart_master_charges`
Expected: Compilation successful

---

## Task 9: Add Schema Documentation

**Files:**
- Modify: `dbt/models/marts/schema.yml`

**Step 1: Add mart_master_charges schema definition**

Add at the end of the models list in schema.yml:

```yaml
  - name: mart_master_charges
    description: |
      Master charges mart - single source of truth for all Stripe payment analytics.
      Grain: One row per Stripe charge.

      Data Sources:
      - raw_stripe.charges: Core payment data
      - raw_stripe.refunds: Refund details
      - raw_funnelfox.subscriptions: FunnelFox linkage (nullable)
      - raw_funnelfox.sessions: User session data (nullable)
      - raw_funnelfox.funnels: Funnel metadata (nullable)

      Use cases:
      - Payment success/failure analysis
      - Risk metrics (disputes, fraud blocks, high-risk transactions)
      - Refund tracking
      - Subscription analytics
      - Funnel attribution
      - Replaces: mart_stripe_payments, mart_stripe_risk_metrics, mart_new_subscriptions
    columns:
      - name: charge_id
        description: Stripe charge ID (primary key)
        tests:
          - unique
          - not_null

      - name: payment_intent_id
        description: Payment intent ID (groups retry attempts)

      - name: customer_id
        description: Stripe customer ID

      - name: invoice_id
        description: Stripe invoice ID

      - name: amount_usd
        description: Payment amount in USD
        tests:
          - not_null

      - name: currency
        description: Original currency code

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

      - name: failure_code
        description: Raw Stripe failure code

      - name: failure_category
        description: Categorized failure type
        tests:
          - accepted_values:
              values: ['insufficient_funds', 'card_declined', 'fraud_block', 'authentication_required', 'expired_card', 'invalid_card', 'processing_error', 'technical_error']
              config:
                where: "status = 'failed'"

      - name: recovery_action
        description: Suggested recovery action
        tests:
          - accepted_values:
              values: ['retry_eligible', 'request_new_card', 'verify_3ds', 'contact_support']
              config:
                where: "status = 'failed'"

      - name: card_brand
        description: Card brand (visa, mastercard, etc.)

      - name: card_country
        description: Card issuing country

      - name: card_funding
        description: Card funding type (credit, debit, prepaid)

      - name: card_last4
        description: Last 4 digits of card

      - name: customer_country
        description: Customer billing country

      - name: risk_level
        description: Stripe Radar risk level (normal, elevated, highest)

      - name: risk_score
        description: Stripe Radar risk score (0-100)

      - name: outcome_type
        description: Payment outcome type (authorized, blocked, etc.)

      - name: outcome_reason
        description: Outcome reason if blocked

      - name: is_blocked
        description: True if payment was blocked by Stripe Radar
        tests:
          - not_null

      - name: is_fraud_decline
        description: True if declined for fraud-related reasons
        tests:
          - not_null

      - name: is_disputed
        description: True if charge has a dispute/chargeback
        tests:
          - not_null

      - name: dispute_id
        description: Stripe dispute ID if disputed

      - name: refund_count
        description: Number of refunds on this charge
        tests:
          - not_null

      - name: refund_amount_usd
        description: Total refunded amount in USD
        tests:
          - not_null

      - name: is_fully_refunded
        description: True if fully refunded
        tests:
          - not_null

      - name: refund_ratio
        description: Refund amount / charge amount

      - name: latest_refund_date
        description: Date of most recent refund

      - name: latest_refund_reason
        description: Reason for most recent refund

      - name: profile_id
        description: FunnelFox user profile ID (nullable)

      - name: ff_subscription_id
        description: FunnelFox subscription ID (nullable)

      - name: ff_subscription_status
        description: FunnelFox subscription status (nullable)

      - name: payment_provider
        description: Payment provider (stripe, apple, google)

      - name: billing_interval
        description: Billing interval (month, year, etc.)

      - name: billing_interval_count
        description: Number of billing intervals

      - name: subscription_price_usd
        description: FunnelFox subscription price in USD

      - name: funnel_id
        description: Funnel ID (nullable)

      - name: funnel_title
        description: Funnel name (nullable)

      - name: funnel_type
        description: Funnel type (nullable)

      - name: funnel_environment
        description: Funnel environment (nullable)

      - name: country
        description: User country from session (nullable)

      - name: city
        description: User city from session (nullable)

      - name: traffic_source
        description: Traffic source/origin (nullable)

      - name: first_session_at
        description: First session timestamp (nullable)

      - name: created_at
        description: Charge creation timestamp
        tests:
          - not_null

      - name: created_date
        description: Charge date (UTC)
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

      - name: is_organic
        description: True if no FunnelFox linkage (organic/direct)
        tests:
          - not_null
```

**Step 2: Verify schema is valid YAML**

Run: `cd dbt && dbt parse`
Expected: Parsing successful

---

## Task 10: Run and Validate the Model

**Files:**
- None (validation only)

**Step 1: Run the model**

Run: `cd dbt && dbt run --select mart_master_charges`
Expected: Model runs successfully

**Step 2: Run tests**

Run: `cd dbt && dbt test --select mart_master_charges`
Expected: All tests pass

**Step 3: Validate row count**

Run: `cd dbt && dbt run-operation --args "{'sql': 'SELECT COUNT(*) FROM dbt_analytics.mart_master_charges'}" run_query`

Or manually check in database.

**Step 4: Commit**

```bash
git add dbt/models/marts/mart_master_charges.sql dbt/models/marts/schema.yml
git commit -m "feat: add mart_master_charges - single source of truth for payment analytics

Consolidates payment, subscription, and risk metrics into one charge-level
table. Includes FunnelFox linkage (nullable), refund details from refunds
table, and risk metrics using EFW proxy approach.

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"
```

---

## Task 11: Update sources.yml (if needed)

**Files:**
- Check: `dbt/models/sources.yml`

**Step 1: Verify raw_stripe.refunds is defined**

Check if `refunds` table exists in sources.yml under raw_stripe. If not, add it.

**Step 2: If missing, add refunds source**

```yaml
      - name: refunds
        description: Stripe refunds
        columns:
          - name: id
            description: Refund ID
          - name: charge
            description: Related charge ID
          - name: amount
            description: Refund amount in cents
          - name: currency
            description: Currency code
          - name: status
            description: Refund status (pending, succeeded, failed, canceled)
          - name: created
            description: Refund creation timestamp
          - name: reason
            description: Refund reason
```

---

## Summary

| Task | Description | Files |
|------|-------------|-------|
| 1 | Create base model structure | mart_master_charges.sql |
| 2 | Build core charges CTE | mart_master_charges.sql |
| 3 | Add refund details CTE | mart_master_charges.sql |
| 4 | Add FunnelFox subscription CTE | mart_master_charges.sql |
| 5 | Add first session CTE | mart_master_charges.sql |
| 6 | Add funnels metadata CTE | mart_master_charges.sql |
| 7 | Add failure categorization CTE | mart_master_charges.sql |
| 8 | Build final SELECT with joins | mart_master_charges.sql |
| 9 | Add schema documentation | schema.yml |
| 10 | Run and validate model | - |
| 11 | Update sources if needed | sources.yml |

**Total estimated tasks:** 11
**Key validation:** dbt run + dbt test pass
