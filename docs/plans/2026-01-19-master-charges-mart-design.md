# Master Charges Mart Design

**Date:** 2026-01-19
**Status:** Draft
**Purpose:** Single source of truth for all Stripe payment analytics

## Overview

Create a unified charge-centric data mart (`mart_master_charges`) that consolidates payment, subscription, and risk metrics into a single table. This replaces multiple existing marts to improve consistency and reduce maintenance overhead.

## Design Decisions

| Aspect | Decision | Rationale |
|--------|----------|-----------|
| **Grain** | One row per Stripe charge | Lowest level for payment analysis |
| **Scope** | All Stripe charges | FunnelFox fields nullable for unlinked charges |
| **Risk data** | Proxy from outcome JSON | Same approach as current mart_stripe_risk_metrics |
| **Refunds** | Aggregates + latest details | Join refunds table for latest_refund_date/reason |
| **Disputes** | Embedded disputed flag | From charges table directly |
| **Intent tracking** | payment_intent_id as FK only | No pre-computed intent aggregates |
| **FunnelFox linkage** | subscriptions.psp_id = charges.id | Standard linkage |
| **Session attribution** | First session per profile | For traffic source attribution |

## Table Schema

### Core Charge Fields

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `charge_id` | TEXT | charges.id | Primary key |
| `payment_intent_id` | TEXT | charges.payment_intent | Groups retry attempts |
| `customer_id` | TEXT | charges.customer | Stripe customer |
| `invoice_id` | TEXT | charges.invoice | Linked invoice |
| `amount_usd` | NUMERIC | charges.amount / 100 | Amount in USD |
| `currency` | TEXT | charges.currency | Original currency |
| `status` | TEXT | charges.status | succeeded/failed/pending |
| `is_successful` | BOOLEAN | derived | status = 'succeeded' |
| `failure_code` | TEXT | charges.failure_code | Raw failure code |
| `created_at` | TIMESTAMP | charges.created | Charge timestamp |
| `created_date` | DATE | derived | Date only |

### Card & Customer Info

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `card_brand` | TEXT | payment_method_details.card.brand | visa, mastercard, etc. |
| `card_country` | TEXT | payment_method_details.card.country | Card issuing country |
| `card_funding` | TEXT | payment_method_details.card.funding | credit, debit, prepaid |
| `card_last4` | TEXT | payment_method_details.card.last4 | Last 4 digits |

### Risk Fields

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `risk_level` | TEXT | outcome.risk_level | normal, elevated, highest |
| `risk_score` | INT | outcome.risk_score | 0-100 numeric score |
| `outcome_type` | TEXT | outcome.type | authorized, blocked, etc. |
| `outcome_reason` | TEXT | outcome.reason | Block reason if applicable |
| `is_blocked` | BOOLEAN | derived | outcome_type = 'blocked' |
| `is_fraud_decline` | BOOLEAN | derived | failure_code in fraud codes |

### Dispute Fields

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `is_disputed` | BOOLEAN | charges.disputed | Has chargeback |
| `dispute_id` | TEXT | charges.dispute | Dispute ID if exists |

### Refund Fields

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `is_refunded` | BOOLEAN | charges.refunded | Has any refund |
| `refund_amount_usd` | NUMERIC | charges.amount_refunded / 100 | Total refunded |
| `refund_count` | INT | COUNT from refunds | Number of refunds |
| `is_fully_refunded` | BOOLEAN | derived | refund_amount = amount |
| `latest_refund_date` | TIMESTAMP | MAX(refunds.created) | Most recent refund |
| `latest_refund_reason` | TEXT | refunds.reason | Latest refund reason |

### FunnelFox Linkage (nullable)

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `profile_id` | TEXT | ff_subscriptions.profile_id | FunnelFox user ID |
| `ff_subscription_id` | TEXT | ff_subscriptions.id | FunnelFox subscription ID |
| `ff_subscription_status` | TEXT | ff_subscriptions.status | Subscription status |
| `payment_provider` | TEXT | ff_subscriptions.payment_provider | stripe, apple, google |
| `billing_interval` | TEXT | ff_subscriptions.billing_interval | month, year, etc. |
| `billing_interval_count` | INT | ff_subscriptions.billing_interval_count | Number of intervals |
| `subscription_price_usd` | NUMERIC | ff_subscriptions.price / 100 | Subscription price |

### Funnel & Traffic Info (nullable)

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `funnel_id` | TEXT | ff_funnels.id | Funnel ID |
| `funnel_title` | TEXT | ff_funnels.title | Funnel name |
| `funnel_type` | TEXT | ff_funnels.type | Funnel type |
| `funnel_environment` | TEXT | ff_funnels.environment | dev, staging, prod |
| `country` | TEXT | ff_sessions.country | User's country |
| `city` | TEXT | ff_sessions.city | User's city |
| `traffic_source` | TEXT | ff_sessions.origin | Traffic source |

### Time Dimensions

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `created_at` | TIMESTAMP | charges.created | Full timestamp (UTC) |
| `created_date` | DATE | DATE(created) | Date only |
| `hour_of_day` | INT | EXTRACT(hour) | 0-23 |
| `day_of_week` | TEXT | TO_CHAR(dow) | Monday, Tuesday, etc. |
| `week_start_date` | DATE | DATE_TRUNC('week') | Monday of the week |
| `month_start_date` | DATE | DATE_TRUNC('month') | First of month |

### Derived Categories

| Column | Type | Description |
|--------|------|-------------|
| `failure_category` | TEXT | Grouped: insufficient_funds, card_declined, fraud_block, authentication_required, expired_card, invalid_card, processing_error, technical_error |
| `recovery_action` | TEXT | Suggested: retry_eligible, request_new_card, verify_3ds, contact_support |

## Join Logic

```sql
FROM raw_stripe.charges c

-- Refund details (aggregated per charge)
LEFT JOIN (
    SELECT
        charge,
        COUNT(*) as refund_count,
        MAX(created) as latest_refund_date,
        -- latest refund reason via ROW_NUMBER
    FROM raw_stripe.refunds
    GROUP BY charge
) ref ON ref.charge = c.id

-- FunnelFox subscription linkage
LEFT JOIN raw_funnelfox.subscriptions ff_sub
    ON ff_sub.psp_id = c.id
    AND ff_sub.sandbox = false

-- First session for the user (for attribution)
LEFT JOIN (
    SELECT DISTINCT ON (profile_id)
        profile_id,
        funnel_id,
        country,
        city,
        origin as traffic_source,
        created_at as first_session_at
    FROM raw_funnelfox.sessions
    ORDER BY profile_id, created_at ASC
) first_session ON first_session.profile_id = ff_sub.profile_id

-- Funnel metadata
LEFT JOIN raw_funnelfox.funnels f
    ON f.id = first_session.funnel_id
```

## Filters

- Exclude sandbox: `ff_subscriptions.sandbox = false`
- Exclude test amounts: `amount NOT IN (100, 200)`

## Replacing Existing Marts

| Current Mart | Derivation from Master |
|--------------|------------------------|
| `mart_stripe_payments` | Direct subset - same grain, columns map 1:1 |
| `mart_stripe_payments_daily` | `GROUP BY created_date, funnel_title, card_country, card_brand` |
| `mart_stripe_risk_metrics` | `GROUP BY created_date` with risk/refund/dispute aggregations |
| `mart_new_subscriptions` | `WHERE is_successful AND ff_subscription_id IS NOT NULL` |

### Example: Risk Metrics Query

```sql
SELECT
    created_date as date,
    COUNT(*) FILTER (WHERE is_disputed AND card_brand = 'visa') as chb_count_visa,
    COUNT(*) FILTER (WHERE is_disputed AND card_brand = 'mastercard') as chb_count_mastercard,
    COUNT(*) FILTER (WHERE risk_level IN ('elevated', 'highest')) as high_risk_count,
    COUNT(*) FILTER (WHERE is_refunded) as refund_count,
    SUM(refund_amount_usd) as refund_amount_usd
FROM mart_master_charges
GROUP BY created_date
```

## Migration Plan

1. **Build** - Create `mart_master_charges` alongside existing marts
2. **Validate** - Create views replicating existing marts from master, compare metrics
3. **Migrate** - Update dashboard tabs one by one to query master mart
4. **Deprecate** - Remove old marts once all tabs migrated

## File Location

`dbt/models/marts/mart_master_charges.sql`
