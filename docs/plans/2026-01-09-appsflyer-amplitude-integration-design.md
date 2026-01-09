# AppsFlyer → Amplitude Integration Design

**Date:** 2026-01-09
**Purpose:** Send mobile attribution data from AppsFlyer to Amplitude for in-app analytics and unified user journey tracking

## Overview

Direct server-to-server integration between AppsFlyer and Amplitude to enrich app analytics with attribution context.

**Goals:**
- Attribution in app analytics - see which ad campaigns drove users
- Unified user journey - combine install attribution with product events
- Future: cost/ROI analysis foundation

**Platforms:** iOS and Android

## Architecture

```
┌─────────────────┐         ┌─────────────────┐
│   AppsFlyer     │ ──────► │    Amplitude    │
│  (Attribution)  │  S2S    │   (Analytics)   │
└─────────────────┘         └─────────────────┘
        │                           │
        │ User installs app         │ User properties enriched
        │ Attribution determined    │ with attribution data
        ▼                           ▼
   Media Source              Every Amplitude event
   Campaign                  now has attribution
   Ad Set                    context attached
   Ad Creative
```

**How it works:**
1. User clicks an ad and installs the app
2. AppsFlyer SDK determines attribution (which campaign, source, etc.)
3. AppsFlyer sends attribution data server-to-server to Amplitude
4. Amplitude attaches this as user properties to the user profile
5. All subsequent events from that user carry attribution context

**Data passed to Amplitude:**
- `af_media_source` - Facebook, Google, TikTok, organic, etc.
- `af_campaign` - Campaign name
- `af_adset` - Ad set name
- `af_ad` - Creative name
- `af_channel` - Channel identifier
- `install_time` - When attribution occurred

## Identity Connection

**Current Identity System:**

```
┌─────────────────┐
│   FunnelFox     │
│   profile_id    │ ◄─── Master ID
└────────┬────────┘
         │
    ┌────┴────┬──────────────┐
    ▼         ▼              ▼
┌────────┐ ┌────────┐  ┌──────────┐
│ Stripe │ │Amplitude│  │AppsFlyer │
│ psp_id │ │ user_id │  │customer_ │
│        │ │         │  │ user_id  │
└────────┘ └─────────┘  └──────────┘
```

**Key:** AppsFlyer's `customer_user_id` must equal the `profile_id` used across all systems.

## Implementation Plan

### Part 1: Dashboard Configuration

#### AppsFlyer Dashboard

| Step | Action | Details |
|------|--------|---------|
| 1.1 | Go to Configuration → Integrated Partners | Search "Amplitude" |
| 1.2 | Enable Amplitude integration | Toggle on |
| 1.3 | Enter Amplitude API Key | From Amplitude Settings → Projects |
| 1.4 | Select apps | Enable for both iOS and Android |
| 1.5 | Enable Install postback | Under "In-app events postback" |
| 1.6 | Map user ID | Set Customer User ID → Amplitude User ID |

#### Amplitude Dashboard

| Step | Action | Details |
|------|--------|---------|
| 2.1 | Go to Settings → Projects | Select your project |
| 2.2 | Copy API Key | Give to AppsFlyer in step 1.3 |
| 2.3 | Verify user properties schema | After testing, confirm `af_*` properties appear |

### Part 2: App Code Changes

#### iOS (Swift)

```swift
import AppsFlyerLib
import Amplitude

class AttributionManager {

    // Call this when user authenticates (login/signup)
    func setUserIdentity(profileId: String) {
        // 1. Set AppsFlyer customer user ID
        AppsFlyerLib.shared().customerUserID = profileId

        // 2. Set Amplitude user ID (should already be doing this)
        Amplitude.instance().setUserId(profileId)
    }

    // Call this in AppDelegate didFinishLaunching
    func configureAppsFlyer() {
        AppsFlyerLib.shared().appsFlyerDevKey = "YOUR_DEV_KEY"
        AppsFlyerLib.shared().appleAppID = "YOUR_APP_ID"
    }
}
```

#### Android (Kotlin)

```kotlin
import com.appsflyer.AppsFlyerLib
import com.amplitude.api.Amplitude

class AttributionManager(private val context: Context) {

    // Call this when user authenticates (login/signup)
    fun setUserIdentity(profileId: String) {
        // 1. Set AppsFlyer customer user ID
        AppsFlyerLib.getInstance().setCustomerUserId(profileId)

        // 2. Set Amplitude user ID (should already be doing this)
        Amplitude.getInstance().userId = profileId
    }

    // Call this in Application onCreate
    fun configureAppsFlyer() {
        AppsFlyerLib.getInstance().init(
            "YOUR_DEV_KEY",
            null,
            context
        )
        AppsFlyerLib.getInstance().start(context)
    }
}
```

#### Timing Notes

- Set customer user ID as early as possible after user authentication
- If set before install is reported, attribution data links automatically
- If user installs anonymously first, AppsFlyer will update when ID is set

## Implementation Checklist

### Dashboard Tasks
- [ ] Get Amplitude API Key
- [ ] Enable Amplitude partner in AppsFlyer
- [ ] Configure install postback
- [ ] Map Customer User ID to Amplitude User ID

### iOS Code Tasks
- [ ] Set `customerUserID` on authentication
- [ ] Verify AppsFlyer SDK config is correct

### Android Code Tasks
- [ ] Set `customerUserId` on authentication
- [ ] Verify AppsFlyer SDK config is correct

### Testing
- [ ] Register test device in AppsFlyer
- [ ] Simulate attributed install
- [ ] Verify `af_*` properties appear in Amplitude user profile

## Result

Once configured, a user's Amplitude profile will show:
- `user_id` = `profile_id` (from your app's Amplitude SDK)
- `af_media_source` = "facebook" (from AppsFlyer integration)
- `af_campaign` = "summer_promo" (from AppsFlyer integration)

All events from this user now carry attribution context for cohort analysis and funnel breakdowns.

## Future: DWH Integration for ROI Analysis

When ready for cost/ROI analysis, pull AppsFlyer data into the DWH via Meltano:
1. Add AppsFlyer extractor to Meltano
2. Create dbt mart joining attribution costs with Stripe revenue
3. Calculate CAC, LTV, ROAS by campaign
