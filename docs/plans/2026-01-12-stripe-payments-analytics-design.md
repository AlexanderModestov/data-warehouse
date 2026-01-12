# Stripe Payments Analytics Design

**Date:** 2026-01-12
**Status:** Approved

## Overview

Comprehensive Stripe payment analytics covering successful and failed payments, with actionable failure intelligence and retry/recovery tracking.

## Goals

- Track all payment attempts with success/failure breakdown
- Categorize failures with actionable recovery suggestions
- Link retry attempts via payment_intent to measure recovery rates
- Full dimensional coverage: time, funnel, geography, card brand
- Two granularity levels: detailed fact table + daily summary

## Architecture

```
raw_stripe.charges
       ↓
mart_stripe_payments (detailed, one row per charge)
       ↓
mart_stripe_payments_daily (aggregated by day + dimensions)
```

**Identity linking:**
- Join to `funnelfox_raw.sessions` via Stripe `customer` → FunnelFox `profile_id`
- Use `funnelfox_raw.subscriptions.psp_id` = `charges.id` for subscription context

## Model 1: mart_stripe_payments (Fact Table)

One row per payment attempt with all dimensions attached.

### Primary Columns

| Column | Type | Description |
|--------|------|-------------|
| `charge_id` | varchar | Primary key (Stripe charge ID) |
| `payment_intent_id` | varchar | Groups retry attempts together |
| `customer_id` | varchar | Stripe customer ID |
| `profile_id` | varchar | FunnelFox master ID (for joins) |

### Payment Outcome

| Column | Type | Description |
|--------|------|-------------|
| `status` | varchar | 'succeeded', 'failed', 'pending' |
| `is_successful` | boolean | True if succeeded |
| `amount_usd` | numeric | Amount in dollars (amount/100) |
| `currency` | varchar | Original currency code |

### Failure Intelligence

| Column | Type | Description |
|--------|------|-------------|
| `failure_code` | varchar | Raw Stripe failure code |
| `failure_category` | varchar | Categorized failure type |
| `recovery_action` | varchar | Suggested next step |

**Failure categories:**
- `insufficient_funds`
- `card_declined`
- `fraud_block`
- `authentication_required`
- `expired_card`
- `invalid_card`
- `processing_error`
- `technical_error`

**Recovery actions:**
- `retry_eligible` - Automated retry makes sense
- `request_new_card` - User action required
- `verify_3ds` - Redirect to authentication
- `contact_support` - Manual intervention needed

### Retry Tracking

| Column | Type | Description |
|--------|------|-------------|
| `attempt_number` | int | 1st, 2nd, 3rd attempt within payment_intent |
| `is_first_attempt` | boolean | True if first attempt |
| `is_final_attempt` | boolean | True if last attempt for this intent |
| `intent_eventually_succeeded` | boolean | Did any attempt in this intent succeed? |

### Time Dimensions

| Column | Type | Description |
|--------|------|-------------|
| `created_at` | timestamp | Payment attempt timestamp (UTC) |
| `created_date` | date | Date only for easy grouping |
| `hour_of_day` | int | 0-23, for time-of-day patterns |
| `day_of_week` | varchar | 'Monday', 'Tuesday', etc. |
| `week_start_date` | date | Monday of that week |
| `month_start_date` | date | First of the month |

### Funnel Dimensions

| Column | Type | Description |
|--------|------|-------------|
| `funnel_name` | varchar | Which funnel drove this user |
| `traffic_source` | varchar | utm_source or referrer |
| `traffic_medium` | varchar | utm_medium |
| `traffic_campaign` | varchar | utm_campaign |

### Geographic Dimensions

| Column | Type | Description |
|--------|------|-------------|
| `card_country` | varchar | Card's country of issue |
| `customer_country` | varchar | Customer billing country |

### Card Details

| Column | Type | Description |
|--------|------|-------------|
| `card_brand` | varchar | 'visa', 'mastercard', 'amex', etc. |

## Model 2: mart_stripe_payments_daily (Summary Table)

One row per day + funnel + card_country + card_brand.

### Dimension Columns

| Column | Type | Description |
|--------|------|-------------|
| `date` | date | The day |
| `funnel_name` | varchar | Funnel (null = all funnels) |
| `card_country` | varchar | Card country (null = all countries) |
| `card_brand` | varchar | Card brand (null = all brands) |

### Volume Metrics

| Column | Type | Description |
|--------|------|-------------|
| `total_attempts` | int | Total payment attempts |
| `successful_payments` | int | Count of succeeded |
| `failed_payments` | int | Count of failed |
| `success_rate` | numeric | successful / total (0.0 - 1.0) |

### Revenue Metrics

| Column | Type | Description |
|--------|------|-------------|
| `gross_revenue_usd` | numeric | Sum of successful payments |
| `failed_revenue_usd` | numeric | Sum of failed attempt amounts |

### Failure Breakdown

| Column | Type | Description |
|--------|------|-------------|
| `failures_card_declined` | int | Count by category |
| `failures_insufficient_funds` | int | |
| `failures_fraud_block` | int | |
| `failures_authentication_required` | int | |
| `failures_technical_error` | int | |
| `failures_other` | int | |

### Recovery Metrics

| Column | Type | Description |
|--------|------|-------------|
| `intents_with_retry` | int | Payment intents with multiple attempts |
| `intents_recovered` | int | Failed intents that eventually succeeded |
| `recovery_rate` | numeric | recovered / intents_with_retry |

## Failure Categorization Logic

| Failure Category | Stripe Codes | Recovery Action |
|------------------|--------------|-----------------|
| `insufficient_funds` | `insufficient_funds`, `card_velocity_exceeded` | `retry_eligible` |
| `card_declined` | `card_declined`, `generic_decline`, `do_not_honor` | `request_new_card` |
| `fraud_block` | `fraudulent`, `merchant_blacklist`, `stolen_card`, `lost_card` | `contact_support` |
| `authentication_required` | `authentication_required`, `card_not_supported` | `verify_3ds` |
| `expired_card` | `expired_card` | `request_new_card` |
| `invalid_card` | `invalid_card_number`, `invalid_cvc`, `invalid_expiry` | `request_new_card` |
| `processing_error` | `processing_error`, `try_again_later` | `retry_eligible` |
| `technical_error` | All other codes, nulls | `contact_support` |

## File Structure

```
transform/models/marts/
├── mart_stripe_payments.sql
├── mart_stripe_payments_daily.sql
└── schema.yml
```

## Source Dependencies

- `raw_stripe.charges` - Payment attempts
- `raw_stripe.payment_intents` - Retry linking (if separate table)
- `funnelfox_raw.sessions` - Funnel context
- `funnelfox_raw.subscriptions` - Profile linking via psp_id

## dbt Tests

- `unique` on `charge_id`
- `not_null` on `charge_id`, `status`, `created_at`
- `accepted_values` on `status` ('succeeded', 'failed', 'pending')
- `accepted_values` on `failure_category`

## Recommended Indexes

- `created_date` - Time-based filtering
- `payment_intent_id` - Retry analysis
- `funnel_name` - Funnel breakdowns

## Implementation Notes

- Revenue calculation: `amount / 100.0` (Stripe stores cents)
- All timestamps in UTC
- Exclude sandbox/test transactions
- Join strategy: charges → subscriptions (via psp_id) → sessions (via profile_id)
