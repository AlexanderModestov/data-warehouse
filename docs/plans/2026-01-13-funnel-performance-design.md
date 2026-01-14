# Funnel Performance Mart Design

## Overview

Create `mart_funnel_performance` to analyze funnel conversion rates by time period and traffic source, with Facebook/Instagram attribution based on user agent detection.

## Requirements

1. Funnel conversion metrics broken down by time period (daily grain, rollup to week/month)
2. Traffic source breakdown with Facebook/Instagram detection
3. Link to Amplitude engagement metrics

## Data Model

### Grain
One row per funnel + date + traffic source

### Primary Key
`date` + `funnel_id` + `traffic_source`

### Traffic Source Detection
```sql
CASE
  WHEN user_agent LIKE '%FBAN/FBIOS%' OR user_agent LIKE '%FB_IAB/FB4A%' THEN 'facebook'
  WHEN user_agent LIKE '%Instagram%' THEN 'instagram'
  WHEN user_agent LIKE '%TikTok%' THEN 'tiktok'
  ELSE 'organic'
END
```

## Schema

| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Daily grain |
| funnel_id | TEXT | Funnel identifier |
| funnel_title | TEXT | Funnel name |
| funnel_type | TEXT | Funnel type |
| traffic_source | TEXT | facebook, instagram, tiktok, organic |
| total_sessions | INTEGER | Count of sessions |
| unique_users | INTEGER | Distinct profile_ids |
| conversions | INTEGER | Successful payments |
| conversion_rate | NUMERIC | conversions / total_sessions |
| revenue_usd | NUMERIC | Sum of revenue |
| avg_hours_to_convert | NUMERIC | Average time to convert |
| users_with_amplitude_events | INTEGER | Users with Amplitude activity |
| avg_events_per_user | NUMERIC | Engagement metric |

## Data Flow

1. Start with `funnelfox.sessions` - parse traffic source from user_agent
2. Join `funnelfox.funnels` for metadata
3. **Data Linkage Discovery:** The `profile_id` column doesn't exist in `funnelfox.subscriptions`. Instead:
   - Stripe subscriptions contain `ff_session_id` in metadata → links to FunnelFox sessions
   - FunnelFox subscriptions link to Stripe subscriptions via `psp_id`
4. Join `stripe.subscriptions` via `ff_session_id` metadata
5. Join `funnelfox.subscriptions` via `psp_id`
6. Join `stripe.charges` for revenue data
7. Join `amplitude.events` for engagement metrics
8. Aggregate by funnel_id, date, traffic_source
