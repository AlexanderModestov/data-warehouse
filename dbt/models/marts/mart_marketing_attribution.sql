{{
    config(
        materialized='table'
    )
}}

/*
    Marketing Attribution Mart

    Purpose: Session-level marketing analytics combining Facebook Ads, FunnelFox sessions,
             Amplitude events, and Stripe payments for ROAS, CAC, and funnel analysis.
    Grain: One row per FunnelFox session

    Data Flow:
    - Facebook Ads (spend/impressions/clicks)
    - FunnelFox Sessions (with UTM params, fbclid, ad_id)
    - Amplitude Events (aggregated per session via ff_session_id)
    - Stripe Payments (revenue)

    Key Joins:
    - Session origin → Parse UTM params → Match to Facebook campaigns/ads
    - Amplitude event_properties->>'ff_session_id' → Session id
    - Session → Subscription → Payment (existing flow)
*/

-- =============================================================================
-- 1. BASE SESSIONS WITH PARSED UTM PARAMETERS
-- =============================================================================
WITH funnelfox_sessions AS (
    SELECT
        id AS session_id,
        profile_id,
        funnel_id,
        created_at AS session_created_at,
        country,
        city,
        origin,
        user_agent,
        ip,
        funnel_version
    FROM {{ source('raw_funnelfox', 'sessions') }}
),

parsed_sessions AS (
    SELECT
        session_id,
        profile_id,
        funnel_id,
        session_created_at,
        country,
        city,
        origin AS origin_raw,
        user_agent,
        ip AS ip_address,
        funnel_version,

        -- Parse UTM parameters from origin field
        -- Returns NULL if parameter not found
        CASE
            WHEN origin LIKE '%utm_source=%'
            THEN SPLIT_PART(SPLIT_PART(origin, 'utm_source=', 2), '&', 1)
            ELSE NULL
        END AS utm_source,

        CASE
            WHEN origin LIKE '%utm_medium=%'
            THEN SPLIT_PART(SPLIT_PART(origin, 'utm_medium=', 2), '&', 1)
            ELSE NULL
        END AS utm_medium,

        CASE
            WHEN origin LIKE '%utm_campaign=%'
            THEN SPLIT_PART(SPLIT_PART(origin, 'utm_campaign=', 2), '&', 1)
            ELSE NULL
        END AS utm_campaign,

        CASE
            WHEN origin LIKE '%utm_content=%'
            THEN SPLIT_PART(SPLIT_PART(origin, 'utm_content=', 2), '&', 1)
            ELSE NULL
        END AS utm_content,

        CASE
            WHEN origin LIKE '%utm_term=%'
            THEN SPLIT_PART(SPLIT_PART(origin, 'utm_term=', 2), '&', 1)
            ELSE NULL
        END AS utm_term,

        CASE
            WHEN origin LIKE '%fbclid=%'
            THEN SPLIT_PART(SPLIT_PART(origin, 'fbclid=', 2), '&', 1)
            ELSE NULL
        END AS fbclid,

        CASE
            WHEN origin LIKE '%ad_id=%'
            THEN SPLIT_PART(SPLIT_PART(origin, 'ad_id=', 2), '&', 1)
            ELSE NULL
        END AS ad_id_from_url

    FROM funnelfox_sessions
),

-- =============================================================================
-- 2. FUNNEL METADATA
-- =============================================================================
funnels AS (
    SELECT
        id AS funnel_id,
        title AS funnel_title,
        type AS funnel_type,
        environment AS funnel_environment
    FROM {{ source('raw_funnelfox', 'funnels') }}
),

-- =============================================================================
-- 3. AMPLITUDE EVENTS AGGREGATED PER SESSION
-- =============================================================================
-- Extract ff_session_id (fsid) from Page Location URL where available
amplitude_with_session AS (
    SELECT
        *,
        SUBSTRING(
            event_properties->>'[Amplitude] Page Location'
            FROM 'fsid=([A-Z0-9]+)'
        ) AS ff_session_id
    FROM {{ source('raw_amplitude', 'events') }}
),

-- Aggregate Amplitude events by session (only events with fsid can be linked)
amplitude_events AS (
    SELECT
        ff_session_id AS session_id,
        COUNT(*) AS total_events,
        COUNT(DISTINCT event_type) AS unique_event_types,
        MIN(event_time) AS first_event_at,
        MAX(event_time) AS last_event_at,
        EXTRACT(EPOCH FROM (MAX(event_time) - MIN(event_time))) AS session_duration_seconds,
        ARRAY_AGG(DISTINCT event_type ORDER BY event_type) AS event_types_list,
        -- Take first non-null device/platform info
        MAX(device_id) AS amplitude_device_id,
        MAX(platform) AS amplitude_platform,
        MAX(os_name) AS amplitude_os_version,
        MAX(country) AS amplitude_country,
        MAX(city) AS amplitude_city
    FROM amplitude_with_session
    WHERE ff_session_id IS NOT NULL
    GROUP BY ff_session_id
),

-- Separate CTE for proper event counts
amplitude_event_counts AS (
    SELECT
        ff_session_id AS session_id,
        JSONB_OBJECT_AGG(event_type, event_count) AS event_counts_json
    FROM (
        SELECT
            ff_session_id,
            event_type,
            COUNT(*) AS event_count
        FROM amplitude_with_session
        WHERE ff_session_id IS NOT NULL
        GROUP BY ff_session_id, event_type
    ) counts
    GROUP BY session_id
),

-- =============================================================================
-- 4. FACEBOOK ADS HIERARCHY
-- =============================================================================
facebook_ads_enriched AS (
    SELECT DISTINCT ON (a.facebook_ad_id)
        a.facebook_ad_id,
        a.facebook_adset_id,
        a.facebook_campaign_id,
        a.ad_name,
        ads.adset_name,
        c.campaign_name,
        c.objective AS campaign_objective
    FROM {{ source('raw_facebook', 'facebook_ads') }} a
    LEFT JOIN {{ source('raw_facebook', 'facebook_adsets') }} ads
        ON a.facebook_adset_id = ads.facebook_adset_id
    LEFT JOIN {{ source('raw_facebook', 'facebook_campaigns') }} c
        ON a.facebook_campaign_id = c.facebook_campaign_id
    ORDER BY a.facebook_ad_id, a.created_time DESC
),

-- Campaign name lookup for secondary matching
facebook_campaigns_lookup AS (
    SELECT DISTINCT ON (campaign_name)
        facebook_campaign_id,
        campaign_name,
        objective AS campaign_objective
    FROM {{ source('raw_facebook', 'facebook_campaigns') }}
    WHERE campaign_name IS NOT NULL
    ORDER BY campaign_name, created_time DESC
),

-- =============================================================================
-- 5. CONVERSIONS (Stripe subscriptions via metadata ff_session_id)
-- =============================================================================
stripe_subscriptions AS (
    SELECT
        id AS stripe_subscription_id,
        customer,
        status,
        created,
        metadata->>'ff_session_id' AS ff_session_id
    FROM {{ source('raw_stripe', 'subscriptions') }}
    WHERE metadata->>'ff_session_id' IS NOT NULL
),

funnelfox_subscriptions AS (
    SELECT
        id AS ff_subscription_id,
        psp_id AS stripe_subscription_id,
        price_usd / 100.0 AS subscription_price_usd,
        sandbox,
        status AS subscription_status,
        created_at AS subscription_created_at
    FROM {{ source('raw_funnelfox', 'subscriptions') }}
    WHERE sandbox = FALSE
),

stripe_charges AS (
    SELECT
        id AS charge_id,
        customer,
        amount / 100.0 AS revenue_usd,
        currency,
        status AS payment_status,
        created AS charge_created_at
    FROM {{ source('raw_stripe', 'charges') }}
    WHERE status = 'succeeded'
),

session_conversions AS (
    SELECT
        ss.ff_session_id AS session_id,
        ss.stripe_subscription_id,
        ffs.subscription_price_usd,
        ffs.subscription_status,
        ffs.subscription_created_at,
        chg.revenue_usd,
        chg.currency,
        chg.payment_status,
        chg.charge_created_at AS conversion_timestamp
    FROM stripe_subscriptions ss
    LEFT JOIN funnelfox_subscriptions ffs
        ON ss.stripe_subscription_id = ffs.stripe_subscription_id
    LEFT JOIN stripe_charges chg
        ON ss.customer = chg.customer
        AND chg.charge_created_at >= ss.created
        AND chg.charge_created_at < ss.created + INTERVAL '1 day'
),

-- =============================================================================
-- 6. FINAL JOIN
-- =============================================================================
final AS (
    SELECT
        -- Session identifiers
        ps.session_id,
        ps.profile_id,
        ps.session_created_at AS session_timestamp,
        DATE(ps.session_created_at AT TIME ZONE 'UTC') AS session_date,

        -- Raw attribution (preserved)
        ps.origin_raw,
        ps.user_agent,
        ps.ip_address,

        -- Parsed UTM parameters
        ps.utm_source,
        ps.utm_medium,
        ps.utm_campaign,
        ps.utm_content,
        ps.utm_term,
        ps.fbclid,
        ps.ad_id_from_url AS ad_id,

        -- Facebook ad hierarchy (with fallback chain)
        COALESCE(fb_direct.facebook_ad_id, fb_campaign.facebook_campaign_id) AS facebook_ad_id,
        fb_direct.facebook_adset_id,
        COALESCE(fb_direct.facebook_campaign_id, fb_campaign.facebook_campaign_id) AS facebook_campaign_id,
        fb_direct.ad_name,
        fb_direct.adset_name,
        COALESCE(fb_direct.campaign_name, fb_campaign.campaign_name) AS campaign_name,
        COALESCE(fb_direct.campaign_objective, fb_campaign.campaign_objective) AS campaign_objective,

        -- Amplitude event aggregates
        COALESCE(amp.total_events, 0) AS total_events,
        COALESCE(amp.unique_event_types, 0) AS unique_event_types,
        amp.first_event_at,
        amp.last_event_at,
        amp.session_duration_seconds,
        amp.event_types_list,
        COALESCE(amp_counts.event_counts_json, '{}'::jsonb) AS event_counts_json,

        -- Amplitude device/platform
        amp.amplitude_device_id,
        amp.amplitude_platform,
        amp.amplitude_os_version,
        amp.amplitude_country,
        amp.amplitude_city,

        -- Funnel context
        ps.funnel_id,
        f.funnel_title,
        f.funnel_type,
        f.funnel_environment,
        ps.funnel_version,
        ps.country,
        ps.city,

        -- Conversion & Revenue
        CASE WHEN conv.session_id IS NOT NULL THEN TRUE ELSE FALSE END AS converted,
        conv.conversion_timestamp,
        CASE
            WHEN conv.conversion_timestamp IS NOT NULL
            THEN EXTRACT(EPOCH FROM (conv.conversion_timestamp - ps.session_created_at)) / 3600.0
            ELSE NULL
        END AS hours_to_convert,
        conv.revenue_usd,
        conv.currency,
        conv.payment_status,
        conv.stripe_subscription_id AS subscription_id,
        conv.subscription_status,

        -- Calculated attribution metrics
        CASE
            WHEN LOWER(ps.utm_medium) IN ('cpc', 'paid', 'ppc', 'paidsocial', 'paid_social')
            THEN TRUE
            ELSE FALSE
        END AS is_paid_traffic,

        CASE
            WHEN LOWER(ps.utm_source) = 'facebook'
                OR LOWER(ps.utm_source) = 'fb'
                OR ps.fbclid IS NOT NULL
            THEN TRUE
            ELSE FALSE
        END AS is_facebook_traffic,

        CASE
            WHEN LOWER(ps.utm_source) IN ('facebook', 'fb') AND LOWER(ps.utm_medium) IN ('cpc', 'paid', 'ppc', 'paidsocial', 'paid_social')
                THEN 'facebook_paid'
            WHEN LOWER(ps.utm_source) IN ('facebook', 'fb')
                THEN 'facebook_organic'
            WHEN ps.fbclid IS NOT NULL
                THEN 'facebook_paid'
            WHEN LOWER(ps.utm_source) IN ('google', 'gclid') AND LOWER(ps.utm_medium) IN ('cpc', 'paid', 'ppc')
                THEN 'google_paid'
            WHEN LOWER(ps.utm_source) = 'google'
                THEN 'google_organic'
            WHEN LOWER(ps.utm_medium) IN ('cpc', 'paid', 'ppc')
                THEN 'other_paid'
            WHEN ps.utm_source IS NOT NULL
                THEN 'other_' || LOWER(ps.utm_source)
            WHEN ps.origin_raw IS NOT NULL AND ps.origin_raw != ''
                THEN 'direct_or_unknown'
            ELSE 'direct'
        END AS attribution_channel

    FROM parsed_sessions ps

    -- Join funnel metadata
    LEFT JOIN funnels f
        ON ps.funnel_id = f.funnel_id

    -- Join Amplitude aggregates
    LEFT JOIN amplitude_events amp
        ON ps.session_id = amp.session_id

    LEFT JOIN amplitude_event_counts amp_counts
        ON ps.session_id = amp_counts.session_id

    -- Join Facebook ads (primary: by ad_id from URL)
    LEFT JOIN facebook_ads_enriched fb_direct
        ON ps.ad_id_from_url = fb_direct.facebook_ad_id

    -- Join Facebook campaigns (secondary: by utm_campaign name)
    LEFT JOIN facebook_campaigns_lookup fb_campaign
        ON ps.utm_campaign = fb_campaign.campaign_name
        AND fb_direct.facebook_ad_id IS NULL  -- Only use if direct match failed

    -- Join conversions
    LEFT JOIN session_conversions conv
        ON ps.session_id = conv.session_id
)

SELECT * FROM final
