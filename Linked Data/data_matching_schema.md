# Data Matching Schema

## Visual Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA FLOW                                      │
└─────────────────────────────────────────────────────────────────────────────┘

  ┌──────────────┐         utm_campaign          ┌──────────────┐
  │   FACEBOOK   │◄────────────────────────────► │  AMPLITUDE   │
  │  Campaigns   │   (campaign_id or name)       │   Events     │
  └──────────────┘                               └──────┬───────┘
                                                        │
                                          device_id = 'fnlfx_' + profile_id
                                                        │
                                                        ▼
  ┌──────────────┐                               ┌──────────────┐
  │    STRIPE    │                               │  FUNNELFOX   │
  │ Subscriptions│                               │   Sessions   │
  └──────┬───────┘                               └──────┬───────┘
         │                                              │
         │  metadata->>'ff_session_id' = session.id     │
         └──────────────────────────────────────────────┘
         │
         │  stripe.subscriptions.id = funnelfox.subscriptions.psp_id
         ▼
  ┌──────────────┐
  │  FUNNELFOX   │
  │ Subscriptions│ ──► price_usd (revenue)
  └──────────────┘
```

## Matching Rules

### 1. AMPLITUDE ↔ FUNNELFOX

```
Amplitude.device_id = 'fnlfx_' + FunnelFox.sessions.profile_id
```

**SQL:**
```sql
CASE WHEN device_id LIKE 'fnlfx_%'
     THEN SUBSTRING(device_id FROM 7)  -- removes 'fnlfx_' prefix
END AS profile_id
```

### 2. AMPLITUDE ↔ FACEBOOK

```
Amplitude.user_properties->>'initial_utm_campaign' = Facebook.facebook_campaign_id
                                            (or)  = Facebook.campaign_name
```

**SQL:**
```sql
LEFT JOIN facebook_campaigns fb_by_id
    ON amp.utm_campaign = fb_by_id.facebook_campaign_id
LEFT JOIN facebook_campaigns fb_by_name
    ON amp.utm_campaign = fb_by_name.campaign_name
    AND fb_by_id.facebook_campaign_id IS NULL  -- fallback
```

### 3. FUNNELFOX ↔ STRIPE (for revenue)

```
Stripe.subscriptions.metadata->>'ff_session_id' = FunnelFox.sessions.id
Stripe.subscriptions.id = FunnelFox.subscriptions.psp_id
```

**SQL:**
```sql
SELECT
    ss.metadata->>'ff_session_id' AS session_id,
    fs.price_usd
FROM raw_stripe.subscriptions ss
JOIN raw_funnelfox.subscriptions fs ON ss.id = fs.psp_id
WHERE ss.metadata->>'ff_session_id' IS NOT NULL
  AND fs.sandbox = false
```

## Join Keys Summary

| Source | Target | Join Key |
|--------|--------|----------|
| Amplitude | FunnelFox Sessions | `device_id` = `'fnlfx_' + profile_id` |
| Amplitude | Facebook Campaigns | `utm_campaign` = `facebook_campaign_id` or `campaign_name` |
| Stripe Subscriptions | FunnelFox Sessions | `metadata->>'ff_session_id'` = `sessions.id` |
| Stripe Subscriptions | FunnelFox Subscriptions | `id` = `psp_id` |
