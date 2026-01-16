# Reluvia Data Warehouse

## Project Overview

Data warehouse for Reluvia using dbt for transformations and Meltano for data ingestion. The warehouse tracks user funnels, subscriptions, and payment analytics.

**dbt Project:** `dwh_analytics`
**Profile:** analytics

## Database Schema

### Source Tables (Raw Data)

#### raw_funnelfox.sessions
User funnel sessions with geo-location and device information.

| Column | Description |
|--------|-------------|
| `id` | Unique session identifier |
| `profile_id` | Master user identifier (primary key for identity resolution) |
| `created_at` | Session creation timestamp |
| `funnel_id` | ID of the funnel the session belongs to |
| `country` | User's country |
| `city` | User's city |
| `origin` | Traffic source |
| `user_agent` | User's browser agent |
| `ip` | User's IP address |
| `funnel_version` | Version of the funnel at time of session |

#### raw_funnelfox.subscriptions
FunnelFox subscription records linking to payment providers.

| Column | Description |
|--------|-------------|
| `id` | FunnelFox subscription ID |
| `psp_id` | Payment Service Provider ID (links to Stripe charge ID) |
| `profile_id` | User profile ID |
| `created_at` | Subscription creation timestamp |
| `status` | Subscription status |
| `sandbox` | Whether this is a sandbox/test transaction (boolean) |
| `payment_provider` | Payment provider (stripe, apple, google) |
| `billing_interval` | Billing interval (month, year, etc.) |
| `billing_interval_count` | Number of billing intervals |
| `price` | Price in cents (divide by 100 for USD) |

#### raw_funnelfox.funnels
Funnel configurations and metadata.

| Column | Description |
|--------|-------------|
| `id` | Unique funnel identifier |
| `title` | Funnel title/name |
| `type` | Funnel type |
| `environment` | Funnel environment (dev, staging, prod) |

#### raw_stripe.charges
Stripe payment charges with retry and outcome tracking.

| Column | Description |
|--------|-------------|
| `id` | Stripe charge ID (links to FunnelFox psp_id) |
| `amount` | Charge amount in cents (divide by 100 for USD) |
| `currency` | Currency code |
| `status` | Charge status (succeeded, failed, pending) |
| `created` | Charge creation timestamp |
| `failure_code` | Failure code if charge failed |
| `payment_intent` | Payment intent ID (groups retry attempts together) |
| `payment_method_details` | JSON: card details (brand, country, funding) |
| `billing_details` | JSON: customer billing address |
| `outcome` | JSON: payment outcome details |
| `customer` | Stripe customer ID |
| `invoice` | Invoice ID |

#### raw_stripe.subscriptions
Stripe subscription records.

| Column | Description |
|--------|-------------|
| `id` | Stripe subscription ID |
| `status` | Subscription status |
| `created` | Subscription creation timestamp |

#### raw_stripe.disputes
Stripe disputes (chargebacks).

| Column | Description |
|--------|-------------|
| `id` | Stripe dispute ID |
| `charge` | Related charge ID |
| `amount` | Dispute amount in cents |
| `currency` | Currency code |
| `status` | Dispute status (needs_response, under_review, won, lost) |
| `reason` | Dispute reason (duplicate, fraudulent, product_not_received, etc.) |
| `created` | Dispute creation timestamp |
| `payment_method_details` | JSON: card details (brand, network) |
| `network_reason_code` | Network-specific reason code |

#### raw_stripe.early_fraud_warnings
Stripe Early Fraud Warnings (EFW).

| Column | Description |
|--------|-------------|
| `id` | Early Fraud Warning ID |
| `charge` | Related charge ID |
| `fraud_type` | Type of fraud (unauthorized_use_of_card, made_with_stolen_card, etc.) |
| `actionable` | Whether action can be taken |
| `created` | EFW creation timestamp |

#### raw_stripe.refunds
Stripe refunds.

| Column | Description |
|--------|-------------|
| `id` | Refund ID |
| `charge` | Related charge ID |
| `amount` | Refund amount in cents |
| `currency` | Currency code |
| `status` | Refund status (pending, succeeded, failed, canceled) |
| `created` | Refund creation timestamp |
| `reason` | Refund reason |

#### raw_amplitude.events
Product events from Amplitude.

| Column | Description |
|--------|-------------|
| `user_id` | User ID (links to FunnelFox profile_id) |
| `event_type` | Type of event |
| `event_time` | Event timestamp |
| `device_id` | Device identifier |

---

### Mart Tables (Analytics Layer)

#### mart_stripe_payments
**Grain:** One row per payment attempt (Stripe charge)
**Primary Key:** `charge_id`

| Column | Type | Description |
|--------|------|-------------|
| `charge_id` | PK | Stripe charge ID |
| `payment_intent_id` | | Groups retry attempts together |
| `customer_id` | | Stripe customer ID |
| `profile_id` | | FunnelFox master user ID (pending invoice linkage) |
| `status` | not null | Payment status (succeeded, failed, pending) |
| `is_successful` | not null | Boolean flag for success |
| `amount_usd` | not null | Payment amount in USD |
| `currency` | | Original currency code |
| `failure_code` | | Raw Stripe failure code |
| `failure_category` | | Categorized: insufficient_funds, card_declined, fraud_block, authentication_required, expired_card, invalid_card, processing_error, technical_error |
| `recovery_action` | | Suggested action: retry_eligible, request_new_card, verify_3ds, contact_support |
| `attempt_number` | not null | Attempt number within payment intent |
| `is_first_attempt` | not null | True if first attempt |
| `is_final_attempt` | not null | True if last attempt |
| `intent_eventually_succeeded` | not null | True if any attempt succeeded |
| `created_at` | not null | Payment attempt timestamp (UTC) |
| `created_date` | not null | Payment date (UTC) |
| `hour_of_day` | | Hour of day (0-23) |
| `day_of_week` | | Day of week name |
| `week_start_date` | | Monday of the week |
| `month_start_date` | | First of the month |
| `funnel_name` | | Funnel name (pending) |
| `traffic_source` | | Traffic source (pending) |
| `traffic_medium` | | Traffic medium (pending) |
| `traffic_campaign` | | Traffic campaign (pending) |
| `card_country` | | Card country of issue |
| `customer_country` | | Customer billing country |
| `card_brand` | | Card brand (visa, mastercard, etc.) |

#### mart_stripe_payments_daily
**Grain:** One row per day + funnel + card_country + card_brand

| Column | Type | Description |
|--------|------|-------------|
| `date` | not null | The date |
| `funnel_name` | | Funnel name |
| `card_country` | | Card country |
| `card_brand` | | Card brand |
| `total_attempts` | not null | Total payment attempts |
| `successful_payments` | not null | Count of successful payments |
| `failed_payments` | not null | Count of failed payments |
| `success_rate` | | Success rate (0.0 - 1.0) |
| `gross_revenue_usd` | | Sum of successful amounts |
| `failed_revenue_usd` | | Sum of failed amounts |
| `failures_insufficient_funds` | | Count by failure type |
| `failures_card_declined` | | Count by failure type |
| `failures_fraud_block` | | Count by failure type |
| `failures_authentication_required` | | Count by failure type |
| `failures_technical_error` | | Count by failure type |
| `failures_other` | | Count by failure type |
| `intents_with_retry` | | Payment intents with multiple attempts |
| `intents_recovered` | | Failed intents that eventually succeeded |
| `recovery_rate` | | Recovery rate |

#### mart_new_subscriptions
**Grain:** One row per new subscription
**Primary Key:** `subscription_id`

| Column | Type | Description |
|--------|------|-------------|
| `subscription_id` | PK | Stripe charge ID for first payment |
| `user_profile_id` | not null | FunnelFox profile ID |
| `funnelfox_subscription_id` | | FunnelFox subscription identifier |
| `subscription_date` | not null | Date of subscription (UTC) |
| `subscription_timestamp` | | Exact timestamp |
| `revenue_usd` | not null | Revenue in USD |
| `currency` | | Currency code |
| `subscription_price_usd` | | FunnelFox subscription price |
| `payment_provider` | | Payment provider |
| `billing_interval` | | Billing interval |
| `billing_interval_count` | | Number of billing intervals |
| `subscription_status` | | FunnelFox subscription status |
| `funnel_id` | | Funnel ID |
| `funnel_title` | | Funnel name |
| `funnel_type` | | Funnel type |
| `funnel_environment` | | Funnel environment |
| `country` | | User's country |
| `city` | | User's city |
| `traffic_source` | | Traffic source/origin |
| `first_session_at` | | First session timestamp |
| `hours_to_convert` | | Hours from first session to subscription |

#### mart_funnel_conversions
**Grain:** One row per funnel session
**Primary Key:** `session_id`

| Column | Type | Description |
|--------|------|-------------|
| `session_id` | PK | FunnelFox session ID |
| `profile_id` | not null | User profile ID |
| `session_date` | not null | Date of session (UTC) |
| `session_timestamp` | | Exact timestamp |
| `funnel_id` | not null | Funnel ID |
| `funnel_title` | | Funnel name |
| `funnel_type` | | Funnel type |
| `funnel_environment` | | Funnel environment |
| `funnel_version` | | Funnel version |
| `country` | | User's country |
| `city` | | User's city |
| `traffic_source` | | Traffic source/origin |
| `converted` | not null | Binary flag (1 = converted, 0 = not) |
| `had_purchase` | not null | Boolean for purchase |
| `revenue_usd` | | Revenue if converted |
| `currency` | | Currency code |
| `time_to_conversion_hours` | | Hours to convert |
| `conversion_timestamp` | | Conversion timestamp |
| `conversion_date` | | Conversion date |

#### mart_stripe_risk_metrics
**Grain:** One row per day
**Primary Key:** `date`

| Column | Type | Description |
|--------|------|-------------|
| `date` | PK | The date |
| `efw_count` | not null | Count of Early Fraud Warnings received |
| `efw_amount_usd` | not null | Total USD amount of charges with EFW |
| `chb_count_mastercard` | not null | Count of Mastercard chargebacks |
| `chb_amount_mastercard` | not null | USD amount of Mastercard chargebacks |
| `chb_count_visa` | not null | Count of Visa chargebacks (excluding RDR) |
| `chb_amount_visa` | not null | USD amount of Visa chargebacks |
| `chb_count_other` | not null | Count of chargebacks for other card brands |
| `chb_amount_other` | not null | USD amount of other chargebacks |
| `total_chb_count` | not null | Total count of all chargebacks |
| `total_chb_amount_usd` | not null | Total USD amount of all chargebacks |
| `rdr_count` | not null | Count of Visa RDR disputes |
| `rdr_amount_usd` | not null | USD amount of Visa RDR disputes |
| `chb_rate_mastercard` | | MC disputes today / MC success prev 30 days |
| `chb_rate_visa` | | Visa disputes today / Visa success prev 30 days |
| `vamp_count` | not null | VAMP count (EFW + Visa chargebacks) |
| `vamp_rate` | | VAMP count / Visa successful transactions |
| `refund_count` | not null | Count of processed refunds |
| `refund_amount_usd` | not null | Total USD amount of refunds |
| `mc_successful_count` | not null | Mastercard successful transactions |
| `visa_successful_count` | not null | Visa successful transactions |
| `total_successful_count` | not null | Total successful transactions |

---

## Key Relationships

```
raw_funnelfox.sessions.profile_id ──┬──> raw_funnelfox.subscriptions.profile_id
                                    │
raw_funnelfox.sessions.funnel_id ───┴──> raw_funnelfox.funnels.id

raw_funnelfox.subscriptions.psp_id ────> raw_stripe.charges.id

raw_stripe.charges.payment_intent ─────> Groups multiple retry attempts

raw_amplitude.events.user_id ──────────> raw_funnelfox.sessions.profile_id
```

## Business Rules

- **Revenue calculation:** Stripe amounts are in cents, divide by 100 for USD
- **Sandbox exclusion:** `sandbox = false` filters test transactions
- **Successful payments:** `status = 'succeeded'`
- **Failure categorization:** Stripe failure codes mapped to actionable categories
- **Retry tracking:** Multiple charges linked via `payment_intent`

## File Locations

- Source definitions: `dbt/models/sources.yml`
- Mart schemas: `dbt/models/marts/schema.yml`
- Mart models: `dbt/models/marts/mart_*.sql`
- Project config: `dbt/dbt_project.yml`
- Meltano config: `meltano/meltano.yml`
