# Dashboard Metrics Calculation Report

## Overview

The Streamlit dashboard displays analytics from three dbt mart tables:
- **Payments Page** → `mart_stripe_payments` + `mart_stripe_payments_daily`
- **Risk Page** → `mart_stripe_risk_metrics`
- **Subscriptions Page** → `mart_new_subscriptions` + `raw_stripe.subscriptions`

---

## 1. Payments Page Metrics

### Data Source
`analytics.mart_stripe_payments` - One row per Stripe charge (payment attempt)

### Key Metrics

| Metric | Algorithm |
|--------|-----------|
| **Gross Revenue** | `SUM(amount_usd) WHERE is_successful = TRUE` |
| **Successful Payments** | `COUNT(*) WHERE is_successful = TRUE` |
| **Failed Payments** | `COUNT(*) WHERE is_successful = FALSE` |
| **Success Rate** | `successful_count / total_attempts` |
| **Total Attempts** | `COUNT(*)` of all charges |
| **Total Refunds** | `SUM(refunded_amount_usd) WHERE is_successful = TRUE` |
| **Refund Rate** | `total_refunds / total_revenue` |
| **Net Revenue** | `SUM(net_revenue_usd) WHERE is_successful = TRUE` (calculated as `amount_usd - refunded_amount_usd`) |
| **Lost Payments** | `COUNT(*) WHERE is_lost_payment = TRUE` (failed with no session success) |
| **Recovered Payments** | `COUNT(*) WHERE is_recovered_failure = TRUE` (first attempt failed but session eventually succeeded) |
| **Session Recovery Rate** | `recovered_count / (lost_count + recovered_count)` |

### Period Comparison
All metrics support period-over-period comparison:
```
delta = ((current - previous) / previous) * 100
```
If previous = 0, delta = 0.

### Failure Categories
Stripe failure codes are mapped to actionable categories:

| Category | Failure Codes |
|----------|---------------|
| `insufficient_funds` | insufficient_funds, card_velocity_exceeded |
| `card_declined` | card_declined, generic_decline, do_not_honor |
| `fraud_block` | fraudulent, merchant_blacklist, stolen_card, lost_card |
| `authentication_required` | authentication_required, card_not_supported |
| `expired_card` | expired_card |
| `invalid_card` | invalid_card_number, invalid_cvc, invalid_expiry_* |
| `processing_error` | processing_error, try_again_later |
| `technical_error` | All other failures |

### Retry/Recovery Logic

The mart tracks payment recovery at two levels:

1. **Intent Level** (same payment_intent):
   - `attempt_number`: Sequential attempt within payment_intent
   - `intent_eventually_succeeded`: TRUE if any charge in the intent succeeded

2. **Session Level** (via ff_session_id):
   - Links charges to sessions using Stripe subscription metadata
   - `session_eventually_succeeded`: TRUE if any payment in session succeeded
   - `is_lost_payment`: Failed AND session never had success
   - `is_recovered_failure`: First attempt failed BUT session eventually succeeded

---

## 2. Risk Metrics Page

### Data Source
`analytics.mart_stripe_risk_metrics` - One row per day

### Refund Metrics

| Metric | Algorithm |
|--------|-----------|
| **Refund Count** | `SUM(refund_count)` across period |
| **Refund Amount** | `SUM(refund_amount_usd)` across period |
| **Refund Rate** | `refund_amount / successful_amount` (calculated in mart: `refund_amount_usd / successful_amount_usd`) |

### Fraud Risk Metrics (Stripe Radar)

| Metric | Algorithm |
|--------|-----------|
| **Elevated Risk** | `COUNT(*) WHERE outcome->risk_level = 'elevated'` |
| **Highest Risk** | `COUNT(*) WHERE outcome->risk_level = 'highest'` |
| **High Risk Txns** | `elevated_risk_count + highest_risk_count` |
| **Avg Risk Score** | `AVG(outcome->risk_score)` where not null |

### Fraud Blocks

| Metric | Algorithm |
|--------|-----------|
| **Blocked by Radar** | `COUNT(*) WHERE outcome->type = 'blocked'` |
| **Fraud Declines** | `COUNT(*) WHERE failure_code IN ('fraudulent', 'merchant_blacklist', 'stolen_card', 'lost_card')` |
| **EFW Proxy** | `high_risk_count + fraud_declined_count` (proxy for Early Fraud Warnings since EFW table unavailable) |

### Chargeback Metrics

| Metric | Algorithm |
|--------|-----------|
| **Mastercard CBs** | `COUNT(*) WHERE disputed = TRUE AND card_brand = 'mastercard'` |
| **Visa CBs** | `COUNT(*) WHERE disputed = TRUE AND card_brand = 'visa'` |
| **Other CBs** | `COUNT(*) WHERE disputed = TRUE AND card_brand NOT IN ('mastercard', 'visa')` |
| **Total Chargebacks** | Sum of all network chargebacks |

### Chargeback Rates

Uses rolling 30-day window for rate calculation:

```sql
chb_rate_mastercard = chb_count_mastercard / mc_successful_prev_30d
chb_rate_visa = chb_count_visa / visa_successful_prev_30d
```

The rolling window is calculated using:
```sql
SUM(mc_successful_count) OVER (
    ORDER BY date
    ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
)
```

### VAMP Metrics (Visa Acquirer Monitoring Program)

| Metric | Algorithm |
|--------|-----------|
| **VAMP Count** | `efw_proxy_count + chb_count_visa` |
| **VAMP Rate** | `vamp_count / visa_successful_count` (same day) |

---

## 3. Subscriptions Page

### Data Sources
- `analytics.mart_new_subscriptions` - New subscription acquisitions
- `raw_stripe.subscriptions` - For cancellation tracking

### New Subscription Metrics

| Metric | Algorithm |
|--------|-----------|
| **New Subscriptions** | `COUNT(*)` from mart_new_subscriptions |
| **Revenue** | `SUM(revenue_usd)` |
| **Unique Subscribers** | `COUNT(DISTINCT COALESCE(user_profile_id, customer_id))` |
| **Avg Hours to Convert** | `AVG(hours_to_convert) WHERE hours_to_convert IS NOT NULL` |

### Subscription Identification
New subscriptions are identified as Stripe charges where:
```sql
status = 'succeeded'
AND description = 'Subscription creation'
AND amount NOT IN (100, 200)  -- Exclude $1, $2 test payments
```

### Cancellation Metrics

| Metric | Algorithm |
|--------|-----------|
| **Cancelled** | `COUNT(*) FROM raw_stripe.subscriptions WHERE status = 'canceled' AND canceled_at IS NOT NULL` |
| **Net Change** | `new_subscriptions - cancelled` |

### Subscription Dynamics (Daily Chart)

Three CTEs combined:
1. **new_subs**: Daily count from mart_new_subscriptions
2. **cancelled_subs**: Daily count where status = 'canceled' grouped by `DATE(canceled_at)`
3. **date_series**: `generate_series(start, end, '1 day')` for continuous dates

```sql
net_subscriptions = new_subscriptions - cancelled_subscriptions
```

### Hours to Convert

Calculated from first FunnelFox session to Stripe charge:
```sql
EXTRACT(EPOCH FROM (subscription_timestamp - first_session_at)) / 3600.0
```

### Data Linkage Strategy

The mart joins Stripe charges to FunnelFox data using three strategies (in priority order):

1. **Direct Match**: `psp_id = charge_id` (ch_...)
2. **Payment Intent Match**: `psp_id = payment_intent` (pi_...)
3. **Profile/Timestamp Match**: Same profile_id within 5-minute window

---

## Common Patterns

### Data Exclusions
All marts exclude test payments: `amount NOT IN (100, 200)` ($1, $2)

### Currency Conversion
Stripe amounts stored in cents, converted to USD: `amount / 100.0`

### Timezone Handling
Timestamps converted to UTC: `DATE(created AT TIME ZONE 'UTC')`

### Caching
All queries use Streamlit cache with 5-minute TTL: `@st.cache_data(ttl=300)`

### Period Comparison Delta
```python
def calc_delta(current, previous):
    if previous == 0:
        return 0
    return ((current - previous) / previous) * 100
```
