# Data Integration Guide

This document explains how we merge data from different sources in the Reluvia Data Warehouse.

## Data Sources Overview

| Source | Purpose | Primary Keys |
|--------|---------|--------------|
| **FunnelFox** | Web funnel sessions, subscriptions, user profiles | `session_id`, `profile_id` |
| **Amplitude** | Product analytics, user behavior events | `uuid` (event), `user_id` |
| **Stripe** | Payment processing, charges, subscriptions | `charge_id`, `subscription_id`, `customer_id` |
| **Facebook Ads** | Ad campaigns, spend, performance metrics | `facebook_ad_id`, `facebook_campaign_id` |

## Identity Resolution

The **master user identifier** is `profile_id` from FunnelFox. All other systems link back to this ID.

```
                    ┌─────────────────┐
                    │   profile_id    │
                    │   (FunnelFox)   │
                    └────────┬────────┘
                             │
        ┌────────────────────┼────────────────────┐
        │                    │                    │
        ▼                    ▼                    ▼
┌───────────────┐    ┌───────────────┐    ┌───────────────┐
│   Amplitude   │    │    Stripe     │    │   Facebook    │
│   user_id     │    │   customer    │    │   (via UTM)   │
└───────────────┘    └───────────────┘    └───────────────┘
```

## Link Methods by Source

### 1. FunnelFox → Amplitude

**Available link fields:**

| Method | Amplitude Field | FunnelFox Field | Grain | Used |
|--------|-----------------|-----------------|-------|------|
| fsid in URL | `event_properties->>'[Amplitude] Page Location'` | `session_id` | Session | Yes |
| user_id | `user_id` | `profile_id` | User | No |
| device_id | `device_id` (pattern: `fnlfx_{profile_id}`) | `profile_id` | User | No |

**Current approach:** We use `fsid` for session-level linking (most granular).

**How it works:**
- When a user visits a FunnelFox funnel, the session ID is passed to Amplitude via URL parameter
- Amplitude captures this in `event_properties->>'[Amplitude] Page Location'`
- We extract it using regex: `fsid=([A-Z0-9]+)`

**SQL pattern:**
```sql
-- Extract session ID from Amplitude events
SELECT
    SUBSTRING(
        event_properties->>'[Amplitude] Page Location'
        FROM 'fsid=([A-Z0-9]+)'
    ) AS ff_session_id
FROM raw_amplitude.events
WHERE event_properties->>'[Amplitude] Page Location' LIKE '%fsid=%'
```

**Alternative linking (not currently used):**
```sql
-- User-level via user_id
WHERE amplitude.user_id = funnelfox.profile_id

-- User-level via device_id pattern
WHERE amplitude.device_id = 'fnlfx_' || funnelfox.profile_id
```

**Coverage:** Not all Amplitude events have `fsid` - only web funnel events. App events may only have `user_id`.

---

### 2. FunnelFox → Stripe

**Link field:** `ff_session_id` in Stripe subscription metadata

**How it works:**
- When a user subscribes through a FunnelFox funnel, the session ID is stored in Stripe subscription metadata
- FunnelFox also stores `psp_id` (Payment Service Provider ID) which links to Stripe

**SQL pattern:**
```sql
-- Link via Stripe subscription metadata
SELECT
    metadata->>'ff_session_id' AS ff_session_id,
    id AS stripe_subscription_id
FROM raw_stripe.subscriptions
WHERE metadata->>'ff_session_id' IS NOT NULL

-- Link via FunnelFox subscription psp_id
SELECT
    ff.id AS funnelfox_subscription_id,
    ff.psp_id AS stripe_subscription_id,
    ff.profile_id
FROM raw_funnelfox.subscriptions ff
```

**Join chain for session → payment:**
```
FunnelFox Session (session_id)
        ↓ [metadata ff_session_id]
Stripe Subscription (subscription_id)
        ↓ [customer_id + timestamp]
Stripe Charge (charge_id, revenue)
```

---

### 3. FunnelFox → Facebook Ads

**Link fields:** UTM parameters and click IDs in session `origin` field

**How it works:**
- Facebook ads include UTM parameters and `fbclid` in destination URLs
- FunnelFox captures these in the session `origin` field
- We parse UTM params and match to Facebook campaign/ad names

**SQL pattern:**
```sql
-- Parse UTM parameters from origin
SELECT
    id AS session_id,
    -- Extract utm_source
    CASE
        WHEN origin LIKE '%utm_source=%'
        THEN SPLIT_PART(SPLIT_PART(origin, 'utm_source=', 2), '&', 1)
    END AS utm_source,
    -- Extract utm_campaign
    CASE
        WHEN origin LIKE '%utm_campaign=%'
        THEN SPLIT_PART(SPLIT_PART(origin, 'utm_campaign=', 2), '&', 1)
    END AS utm_campaign,
    -- Extract fbclid
    CASE
        WHEN origin LIKE '%fbclid=%'
        THEN SPLIT_PART(SPLIT_PART(origin, 'fbclid=', 2), '&', 1)
    END AS fbclid,
    -- Extract ad_id (if passed)
    CASE
        WHEN origin LIKE '%ad_id=%'
        THEN SPLIT_PART(SPLIT_PART(origin, 'ad_id=', 2), '&', 1)
    END AS ad_id
FROM raw_funnelfox.sessions
```

**Facebook matching strategy (fallback chain):**
1. **Primary:** `ad_id` from URL → `facebook_ads.facebook_ad_id`
2. **Secondary:** `utm_campaign` → `facebook_campaigns.campaign_name`
3. **Tertiary:** `utm_content` → `facebook_ads.ad_name`

---

### 4. Amplitude → Stripe (via FunnelFox)

**No direct link.** Must go through FunnelFox:

```
Amplitude Event
    ↓ [fsid in Page Location]
FunnelFox Session (session_id)
    ↓ [metadata ff_session_id]
Stripe Subscription
    ↓ [customer_id]
Stripe Charge
```

---

### 5. Facebook Ads → Stripe (via FunnelFox)

**No direct link.** Must go through FunnelFox sessions:

```
Facebook Ad (campaign_name, ad_id)
    ↓ [UTM params in origin]
FunnelFox Session
    ↓ [ff_session_id in metadata]
Stripe Subscription → Charge
```

---

## Complete Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           FACEBOOK ADS                                   │
│  facebook_campaigns → facebook_adsets → facebook_ads                    │
│  (spend, impressions, clicks by campaign/adset/ad)                      │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                    UTM params: utm_source, utm_campaign
                    Click IDs: fbclid, ad_id
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         FUNNELFOX SESSIONS                              │
│  session_id (PK), profile_id, funnel_id, origin, created_at            │
│  (captures UTM params in origin field)                                  │
└───────────┬─────────────────────────────────────┬───────────────────────┘
            │                                     │
   fsid= param in URL                    ff_session_id in metadata
            │                                     │
            ▼                                     ▼
┌───────────────────────────┐       ┌─────────────────────────────────────┐
│       AMPLITUDE           │       │           STRIPE                     │
│  event_properties:        │       │  subscriptions.metadata:            │
│  [Amplitude] Page Location│       │    ff_session_id → session_id       │
│  contains fsid=SESSION_ID │       │                                     │
│                           │       │  subscriptions.id ← psp_id          │
│  user_properties:         │       │    (FunnelFox subscription link)    │
│  initial_utm_source, etc  │       │                                     │
│  initial_fbclid, etc      │       │  charges:                           │
│                           │       │    customer_id, amount, status      │
└───────────────────────────┘       └─────────────────────────────────────┘
```

## Key Tables and Join Fields

| From | To | Join Field | Direction |
|------|-----|------------|-----------|
| FunnelFox Sessions | FunnelFox Funnels | `funnel_id` | session → funnel |
| FunnelFox Sessions | Amplitude Events | `session_id` = `fsid` (extracted) | session ← events |
| FunnelFox Sessions | Stripe Subscriptions | `session_id` = `metadata.ff_session_id` | session → subscription |
| FunnelFox Subscriptions | Stripe Subscriptions | `psp_id` = `subscription_id` | ff_sub → stripe_sub |
| Stripe Subscriptions | Stripe Charges | `customer_id` + timestamp window | subscription → charge |
| FunnelFox Sessions | Facebook Ads | `ad_id` (from origin) = `facebook_ad_id` | session → ad |
| FunnelFox Sessions | Facebook Campaigns | `utm_campaign` (from origin) = `campaign_name` | session → campaign |

## Data Quality Considerations

### Coverage Gaps

| Link | Coverage | Reason |
|------|----------|--------|
| Session → Amplitude | ~60-80% | Not all events have `fsid` in URL |
| Session → Facebook | ~40-60% | Only paid traffic has UTM/fbclid |
| Session → Stripe | ~5-10% | Only converting sessions have subscriptions |

### Handling Missing Links

In marts, we use LEFT JOINs to preserve all sessions even when links are missing:

```sql
FROM funnelfox_sessions s
LEFT JOIN amplitude_events a ON s.session_id = a.ff_session_id  -- NULL if no match
LEFT JOIN facebook_ads fb ON s.ad_id = fb.facebook_ad_id        -- NULL if no match
LEFT JOIN stripe_subscriptions sub ON s.session_id = sub.ff_session_id  -- NULL if no conversion
```

### Deduplication

- **Facebook ads:** Use `DISTINCT ON (facebook_ad_id)` ordered by `created_time DESC` to get latest
- **Amplitude per session:** Aggregate events with `GROUP BY ff_session_id`
- **Stripe charges:** Filter by `status = 'succeeded'` and use timestamp window

## Mart Usage

### mart_marketing_attribution

Primary mart combining all sources at session level:

```sql
SELECT
    session_id,
    profile_id,
    -- Facebook attribution
    campaign_name,
    ad_name,
    -- Amplitude engagement
    total_events,
    session_duration_seconds,
    -- Conversion
    converted,
    revenue_usd,
    -- Derived
    attribution_channel
FROM analytics.mart_marketing_attribution
WHERE is_facebook_traffic = TRUE
  AND converted = TRUE
```

### Common Queries

**ROAS by Campaign:**
```sql
SELECT
    campaign_name,
    COUNT(*) AS sessions,
    SUM(CASE WHEN converted THEN 1 ELSE 0 END) AS conversions,
    SUM(revenue_usd) AS revenue
FROM analytics.mart_marketing_attribution
WHERE campaign_name IS NOT NULL
GROUP BY campaign_name
-- Join to facebook_ad_statistics for spend to calculate ROAS
```

**Conversion by Attribution Channel:**
```sql
SELECT
    attribution_channel,
    COUNT(*) AS sessions,
    AVG(CASE WHEN converted THEN 1.0 ELSE 0.0 END) AS conversion_rate,
    AVG(total_events) AS avg_engagement
FROM analytics.mart_marketing_attribution
GROUP BY attribution_channel
ORDER BY conversion_rate DESC
```
