# Marketing Performance Metrics

This document describes all metrics available on the **Marketing Performance** tab and how each is calculated.

**Source Model:** `mart_marketing_performance`
**Grain:** One row per date + campaign + adset + ad

---

## Dimension Fields

| Metric | Description |
|--------|-------------|
| `date` | The calendar date of the metrics |
| `facebook_campaign_id` | Facebook campaign identifier |
| `facebook_adset_id` | Facebook adset identifier |
| `facebook_ad_id` | Facebook ad identifier |
| `campaign_name` | Facebook campaign name |
| `adset_name` | Facebook adset name |
| `ad_name` | Facebook ad creative name (Creo) |
| `campaign_objective` | Campaign objective (e.g., conversions, traffic) |
| `campaign_status` | Campaign status (active, paused, etc.) |
| `adset_status` | Adset status |
| `ad_status` | Ad status |
| `target_countries` | Target countries for the adset |

---

## Core Spend Metrics

| Metric | Calculation | Description |
|--------|-------------|-------------|
| `spend_usd` | `SUM(amount_spent)` | Total ad spend in USD |
| `impressions` | `SUM(impressions)` | Total number of times the ad was displayed |
| `clicks` | `SUM(clicks)` | Total link clicks |
| `unique_clicks` | `SUM(unique_clicks)` | Unique users who clicked |
| `cpm` | `(spend_usd / impressions) * 1000` | Cost Per Mille - cost per 1,000 impressions |
| `cpc` | `spend_usd / clicks` | Cost Per Click |
| `ctr` | `clicks / impressions` | Click-Through Rate - percentage of impressions that resulted in clicks |

---

## Video Metrics

| Metric | Calculation | Description |
|--------|-------------|-------------|
| `video_3_sec_plays` | `SUM(video_3_sec_plays)` | Video plays of at least 3 seconds |
| `thru_plays` | `SUM(thru_plays)` | Video plays to completion (or 15+ seconds) |
| `hook_rate` | `video_3_sec_plays / impressions` | Percentage of impressions that watched 3+ seconds. Measures creative "hook" effectiveness |
| `hold_rate` | `thru_plays / video_3_sec_plays` | Percentage of 3-sec viewers who watched the full video. Measures content retention |

---

## Install Metrics (AppsFlyer)

| Metric | Calculation | Description |
|--------|-------------|-------------|
| `installs` | `COUNT(*)` from AppsFlyer | Mobile app installs attributed to Facebook |
| `cost_per_install` | `spend_usd / installs` | Cost per app install |
| `click_to_install_rate` | `installs / clicks` | Percentage of clicks that resulted in installs |

> **Note:** AppsFlyer integration is not yet available. These metrics will be populated when `raw_appsflyer.installs` data becomes available.

---

## Facebook-Attributed Conversions

These metrics come from Facebook's own attribution (Pixel events).

| Metric | Calculation | Description |
|--------|-------------|-------------|
| `registrations` | `SUM(registrations_completed)` | Registrations attributed by Facebook Pixel |
| `purchases` | `SUM(purchases)` | Purchases/First-Time Deposits attributed by Facebook |
| `leads` | `SUM(leads)` | Lead events attributed by Facebook |
| `install_to_reg_rate` | `registrations / installs` | Percentage of installs that became registrations |
| `reg_to_ftd_rate` | `purchases / registrations` | Registration to First Deposit rate |
| `cost_per_registration` | `spend_usd / registrations` | Cost per registration |
| `cost_per_ftd` | `spend_usd / purchases` | Cost per first-time deposit/purchase |

---

## Funnel Step Conversion Rates (Amplitude)

These metrics track in-app funnel progression from Amplitude events.

| Metric | Calculation | Description |
|--------|-------------|-------------|
| `cr_1to2_screen` | *Not yet implemented* | First screen to second screen conversion rate |
| `cr_to_paywall` | *Not yet implemented* | Percentage of sessions reaching the paywall |
| `paywall_cvr` | *Not yet implemented* | Paywall conversion rate - users who convert at paywall |
| `upsell_cr` | *Not yet implemented* | Upsell conversion rate |

> **Note:** These require Amplitude event aggregation which is not yet implemented.

---

## Our Attributed Metrics (First-Party Attribution)

These metrics use our own attribution logic from `mart_marketing_attribution`.

| Metric | Calculation | Description |
|--------|-------------|-------------|
| `attributed_profiles` | `COUNT(*)` from mart_marketing_attribution | Total profiles attributed to campaign |
| `attributed_users` | `COUNT(DISTINCT profile_id)` | Unique users attributed to campaign |
| `attributed_conversions` | `SUM(CASE WHEN converted THEN 1 ELSE 0 END)` | Conversions based on our attribution logic |
| `revenue_usd` | `SUM(revenue_usd)` from unique subscriptions | Attributed revenue (each subscription counted exactly once) |

### Attribution Logic

1. Revenue is attributed to the campaign of the user's **first session** to avoid double-counting
2. Each subscription is counted exactly once using `DISTINCT ON (stripe_subscription_id)`
3. Campaign-level metrics are assigned only to the first ad per campaign on each date to prevent duplication when aggregating

---

## Revenue & ROI Metrics

| Metric | Calculation | Description |
|--------|-------------|-------------|
| `roas` | `revenue_usd / spend_usd` | Return On Ad Spend - revenue generated per dollar spent |
| `arpu` | `revenue_usd / attributed_conversions` | Average Revenue Per User |
| `cac` | `spend_usd / attributed_conversions` | Customer Acquisition Cost |
| `roi_6m` | `(ARPU * 6 - CAC) / CAC` | Projected 6-month ROI assuming ARPU as monthly value |
| `roi_12m` | `(ARPU * 12 - CAC) / CAC` | Projected 12-month ROI assuming ARPU as monthly value |
| `ltv_12m` | `ARPU * 12` | Projected 12-month Lifetime Value |

### ROI Calculation Details

```
ROI_6m  = (ARPU * 6 - CAC) / CAC
ROI_12m = (ARPU * 12 - CAC) / CAC
LTV_12m = ARPU * 12
```

Where:
- **ARPU** = revenue_usd / attributed_conversions
- **CAC** = spend_usd / attributed_conversions

---

## Data Sources

| Source | Table | Purpose |
|--------|-------|---------|
| Facebook Ads | `raw_facebook.facebook_ad_statistics` | Spend, impressions, clicks, video metrics |
| Facebook Ads | `raw_facebook.facebook_campaigns` | Campaign metadata |
| Facebook Ads | `raw_facebook.facebook_adsets` | Adset metadata |
| Facebook Ads | `raw_facebook.facebook_ads` | Ad metadata |
| Our Attribution | `mart_marketing_attribution` | Profiles, conversions, revenue |
| AppsFlyer | `raw_appsflyer.installs` | Mobile installs (not yet available) |
| Amplitude | `raw_amplitude.events` | Funnel step events (not yet implemented) |

---

## Abbreviations Reference

| Abbreviation | Full Name |
|--------------|-----------|
| IMPRESS | Impressions |
| CLICKS | Clicks |
| CPM | Cost Per Mille (1000 impressions) |
| CPC | Cost Per Click |
| CTR | Click-Through Rate |
| INSTALL | App Installs |
| REG | Registrations |
| FTD | First-Time Deposit/Purchase |
| Inst2Reg | Install to Registration Rate |
| R2D | Registration to Deposit Rate |
| pCVR | Paywall Conversion Rate |
| ROAS | Return On Ad Spend |
| ARPU | Average Revenue Per User |
| CAC | Customer Acquisition Cost |
| LTV | Lifetime Value |
| ROI | Return On Investment |
