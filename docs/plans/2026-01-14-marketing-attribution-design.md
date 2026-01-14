# Marketing Attribution Mart Design

**Date:** 2026-01-14
**Status:** Approved

## Overview

Session-level marketing analytics combining Facebook Ads spend, FunnelFox funnel sessions, Amplitude product events, and Stripe payments to enable ROAS, CAC, funnel performance, and cohort analysis.

## Data Flow

```
Facebook Ads (spend/impressions/clicks)
        ↓
    [UTM params, fbclid, ad_id]
        ↓
FunnelFox Sessions ← Amplitude Events (via ff_session_id)
        ↓
    [profile_id, psp_id]
        ↓
Stripe Payments (revenue)
```

## Key Joins

1. Session `origin` → Parse UTM params → Match to `facebook_ad_statistics.campaign_name` / `ad_name`
2. Session `origin` → Extract `ad_id` → Match to `facebook_ads.facebook_ad_id`
3. Amplitude `event_properties->>'ff_session_id'` → Session `id`
4. Existing session → subscription → payment flow

## Output Schema

### Grain
One row per FunnelFox session.

### Session & Attribution Core

| Column | Type | Description |
|--------|------|-------------|
| `session_id` | PK | FunnelFox session ID |
| `profile_id` | text | User profile ID |
| `session_timestamp` | timestamp | Session start time |
| `session_date` | date | Date (UTC) |

### Raw Attribution

| Column | Type | Description |
|--------|------|-------------|
| `origin_raw` | text | Original origin field, unmodified |
| `user_agent` | text | Browser/device info |
| `ip_address` | text | User IP |

### Parsed UTM Parameters

| Column | Type | Description |
|--------|------|-------------|
| `utm_source` | text | e.g., "facebook" |
| `utm_medium` | text | e.g., "cpc", "paid" |
| `utm_campaign` | text | Campaign name |
| `utm_content` | text | Ad/creative identifier |
| `utm_term` | text | Keyword (if applicable) |
| `fbclid` | text | Facebook click ID |
| `ad_id` | text | Facebook ad ID from URL |

### Facebook Ad Hierarchy

| Column | Type | Description |
|--------|------|-------------|
| `facebook_ad_id` | text | Matched FB ad ID |
| `facebook_adset_id` | text | Adset ID |
| `facebook_campaign_id` | text | Campaign ID |
| `ad_name` | text | Ad creative name |
| `adset_name` | text | Adset name |
| `campaign_name` | text | Campaign name |
| `campaign_objective` | text | Campaign objective |

### Amplitude Event Aggregates

| Column | Type | Description |
|--------|------|-------------|
| `total_events` | int | Total Amplitude events in session |
| `unique_event_types` | int | Count of distinct event types |
| `first_event_at` | timestamp | First event timestamp |
| `last_event_at` | timestamp | Last event timestamp |
| `session_duration_seconds` | numeric | Time between first and last event |
| `event_types_list` | text[] | Array of event types fired |
| `event_counts_json` | jsonb | `{"page_view": 5, "button_click": 3, ...}` |

### Amplitude Device & Platform

| Column | Type | Description |
|--------|------|-------------|
| `amplitude_device_id` | text | Device identifier |
| `amplitude_platform` | text | iOS, Android, Web |
| `amplitude_os_version` | text | OS version |
| `amplitude_country` | text | Country from Amplitude |
| `amplitude_city` | text | City from Amplitude |

### Funnel Context

| Column | Type | Description |
|--------|------|-------------|
| `funnel_id` | text | Funnel ID |
| `funnel_title` | text | Funnel name |
| `funnel_type` | text | Funnel type |
| `funnel_environment` | text | prod/staging/dev |
| `funnel_version` | text | Version at session time |
| `country` | text | FunnelFox country |
| `city` | text | FunnelFox city |

### Conversion & Revenue

| Column | Type | Description |
|--------|------|-------------|
| `converted` | boolean | Did session convert to subscription? |
| `conversion_timestamp` | timestamp | When conversion happened |
| `hours_to_convert` | numeric | Time from session to conversion |
| `revenue_usd` | numeric | Revenue from this session |
| `currency` | text | Original currency |
| `payment_status` | text | succeeded/failed/pending |
| `subscription_id` | text | Stripe subscription ID |
| `billing_interval` | text | month/year |

### Calculated Attribution Metrics

| Column | Type | Description |
|--------|------|-------------|
| `is_paid_traffic` | boolean | utm_medium in ('cpc','paid','ppc') |
| `is_facebook_traffic` | boolean | utm_source = 'facebook' or fbclid present |
| `attribution_channel` | text | 'facebook_paid', 'google_paid', 'organic', etc. |

## Facebook Matching Strategy

Fallback chain for matching sessions to Facebook ads:

1. **Primary:** `ad_id` from URL → `facebook_ads.facebook_ad_id`
2. **Secondary:** `utm_campaign` → `facebook_campaigns.campaign_name`
3. **Tertiary:** `utm_content` → `facebook_ads.ad_name`

## Business Metrics Enabled

| Metric | Calculation |
|--------|-------------|
| **ROAS** | `SUM(revenue_usd) / SUM(ad_spend)` (join to daily stats) |
| **CAC** | `SUM(ad_spend) / COUNT(converted = true)` |
| **CVR by Campaign** | `SUM(converted) / COUNT(*)` grouped by campaign |
| **Time to Convert** | `AVG(hours_to_convert)` by channel |
| **LTV by Source** | Revenue cohorts by `attribution_channel` |

## Implementation Notes

- Model materialized as `table` (not incremental initially)
- UTM parsing handles missing params gracefully (returns NULL)
- Facebook match uses COALESCE fallback chain
- Amplitude aggregation pre-filters on `ff_session_id IS NOT NULL`
- Single mart approach (no staging models)

## File Changes

- `dbt/models/sources.yml` - Add Facebook and Amplitude table definitions
- `dbt/models/marts/mart_marketing_attribution.sql` - New mart model
- `dbt/models/marts/schema.yml` - Add mart schema definition
