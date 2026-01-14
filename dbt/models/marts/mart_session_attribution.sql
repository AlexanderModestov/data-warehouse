{{
    config(
        materialized='table'
    )
}}

/*
    Session Attribution Mart

    Purpose: Link FunnelFox sessions with marketing attribution data from Amplitude
    Grain: One row per FunnelFox session with first-touch attribution

    Business Logic:
    - Extract first-touch attribution from Amplitude user_properties
    - Link to FunnelFox sessions via fsid (FunnelFox session ID) in Page Location URL
    - Categorize traffic sources for analysis

    Data Linkage:
    - Amplitude event_properties->>'[Amplitude] Page Location' contains fsid parameter
    - fsid = FunnelFox session_id (direct link)
    - Uses first event per session for attribution
*/

WITH amplitude_with_session AS (
    -- Extract fsid (FunnelFox session ID) from Page Location URL
    SELECT
        *,
        -- Extract fsid parameter from URL: ...?fpid=xxx&fsid=SESSION_ID
        SUBSTRING(
            event_properties->>'[Amplitude] Page Location'
            FROM 'fsid=([A-Z0-9]+)'
        ) AS ff_session_id
    FROM {{ source('raw_amplitude', 'events') }}
    WHERE event_properties->>'[Amplitude] Page Location' LIKE '%fsid=%'
),

amplitude_attribution AS (
    -- Get first event per FunnelFox session to capture attribution
    SELECT DISTINCT ON (ff_session_id)
        ff_session_id,
        event_time,

        -- UTM Parameters (from user_properties JSONB)
        NULLIF(user_properties->>'initial_utm_source', 'EMPTY') AS utm_source,
        NULLIF(user_properties->>'initial_utm_medium', 'EMPTY') AS utm_medium,
        NULLIF(user_properties->>'initial_utm_campaign', 'EMPTY') AS utm_campaign,
        NULLIF(user_properties->>'initial_utm_content', 'EMPTY') AS utm_content,
        NULLIF(user_properties->>'initial_utm_term', 'EMPTY') AS utm_term,
        NULLIF(user_properties->>'initial_utm_id', 'EMPTY') AS utm_id,

        -- Click IDs for platform attribution
        NULLIF(user_properties->>'initial_fbclid', 'EMPTY') AS fbclid,
        NULLIF(user_properties->>'initial_gclid', 'EMPTY') AS gclid,
        NULLIF(user_properties->>'initial_msclkid', 'EMPTY') AS msclkid,
        NULLIF(user_properties->>'initial_ttclid', 'EMPTY') AS ttclid,
        NULLIF(user_properties->>'initial_twclid', 'EMPTY') AS twclid,
        NULLIF(user_properties->>'initial_li_fat_id', 'EMPTY') AS li_fat_id,
        NULLIF(user_properties->>'initial_dclid', 'EMPTY') AS dclid,
        NULLIF(user_properties->>'initial_gbraid', 'EMPTY') AS gbraid,
        NULLIF(user_properties->>'initial_wbraid', 'EMPTY') AS wbraid,
        NULLIF(user_properties->>'initial_ko_click_id', 'EMPTY') AS ko_click_id,
        NULLIF(user_properties->>'initial_rtd_cid', 'EMPTY') AS rtd_cid,

        -- Referrer information
        NULLIF(user_properties->>'initial_referrer', 'EMPTY') AS initial_referrer,
        NULLIF(user_properties->>'initial_referring_domain', 'EMPTY') AS initial_referring_domain,
        NULLIF(user_properties->>'referrer', 'EMPTY') AS referrer,
        NULLIF(user_properties->>'referring_domain', 'EMPTY') AS referring_domain

    FROM amplitude_with_session
    WHERE ff_session_id IS NOT NULL
    ORDER BY ff_session_id, event_time ASC
),

funnelfox_sessions AS (
    SELECT
        id AS session_id,
        profile_id,
        funnel_id,
        created_at AS session_created_at,
        country,
        city,
        origin,
        user_agent
    FROM {{ source('raw_funnelfox', 'sessions') }}
),

funnels AS (
    SELECT
        id AS funnel_id,
        title AS funnel_title,
        type AS funnel_type,
        environment AS funnel_environment
    FROM {{ source('raw_funnelfox', 'funnels') }}
),

-- Join sessions with attribution via ff_session_id (direct session link)
sessions_with_attribution AS (
    SELECT
        s.session_id,
        s.profile_id,
        s.funnel_id,
        s.session_created_at,
        s.country,
        s.city,
        s.origin,
        s.user_agent,

        -- Attribution data from Amplitude
        a.utm_source,
        a.utm_medium,
        a.utm_campaign,
        a.utm_content,
        a.utm_term,
        a.utm_id,

        -- Click IDs
        a.fbclid,
        a.gclid,
        a.msclkid,
        a.ttclid,
        a.twclid,
        a.li_fat_id,

        -- Referrer
        a.initial_referrer,
        a.initial_referring_domain,

        -- Derived: Traffic source category
        CASE
            WHEN a.fbclid IS NOT NULL OR a.utm_source ILIKE '%facebook%' OR a.utm_source ILIKE '%fb%'
                THEN 'Facebook Ads'
            WHEN a.gclid IS NOT NULL OR a.utm_source ILIKE '%google%'
                THEN 'Google Ads'
            WHEN a.msclkid IS NOT NULL OR a.utm_source ILIKE '%bing%'
                THEN 'Microsoft Ads'
            WHEN a.ttclid IS NOT NULL OR a.utm_source ILIKE '%tiktok%'
                THEN 'TikTok Ads'
            WHEN a.twclid IS NOT NULL OR a.utm_source ILIKE '%twitter%' OR a.utm_source ILIKE '%x.com%'
                THEN 'Twitter/X Ads'
            WHEN a.li_fat_id IS NOT NULL OR a.utm_source ILIKE '%linkedin%'
                THEN 'LinkedIn Ads'
            WHEN a.utm_medium ILIKE '%paid%' OR a.utm_medium ILIKE '%cpc%' OR a.utm_medium ILIKE '%ppc%'
                THEN 'Paid (Other)'
            WHEN a.utm_medium ILIKE '%email%' OR a.utm_source ILIKE '%email%'
                THEN 'Email'
            WHEN a.utm_medium ILIKE '%social%' OR a.initial_referring_domain ILIKE '%facebook%'
                OR a.initial_referring_domain ILIKE '%instagram%' OR a.initial_referring_domain ILIKE '%twitter%'
                THEN 'Organic Social'
            WHEN a.utm_medium ILIKE '%organic%' OR a.utm_source ILIKE '%organic%'
                THEN 'Organic Search'
            WHEN a.initial_referring_domain IS NOT NULL AND a.initial_referring_domain != ''
                THEN 'Referral'
            WHEN a.utm_source IS NULL AND a.initial_referrer IS NULL
                THEN 'Direct'
            ELSE 'Other'
        END AS traffic_source_category,

        -- Derived: Is paid traffic
        CASE
            WHEN a.fbclid IS NOT NULL OR a.gclid IS NOT NULL OR a.msclkid IS NOT NULL
                OR a.ttclid IS NOT NULL OR a.twclid IS NOT NULL OR a.li_fat_id IS NOT NULL
                OR a.utm_medium ILIKE '%paid%' OR a.utm_medium ILIKE '%cpc%' OR a.utm_medium ILIKE '%ppc%'
                THEN TRUE
            ELSE FALSE
        END AS is_paid_traffic,

        -- Derived: Ad platform
        CASE
            WHEN a.fbclid IS NOT NULL THEN 'Facebook'
            WHEN a.gclid IS NOT NULL THEN 'Google'
            WHEN a.msclkid IS NOT NULL THEN 'Microsoft'
            WHEN a.ttclid IS NOT NULL THEN 'TikTok'
            WHEN a.twclid IS NOT NULL THEN 'Twitter'
            WHEN a.li_fat_id IS NOT NULL THEN 'LinkedIn'
            ELSE NULL
        END AS ad_platform

    FROM funnelfox_sessions s
    LEFT JOIN amplitude_attribution a
        ON s.session_id = a.ff_session_id
),

final AS (
    SELECT
        -- Session identifiers
        sa.session_id,
        sa.profile_id,
        DATE(sa.session_created_at AT TIME ZONE 'UTC') AS session_date,
        sa.session_created_at AS session_timestamp,

        -- Funnel context
        sa.funnel_id,
        f.funnel_title,
        f.funnel_type,
        f.funnel_environment,

        -- Geography
        sa.country,
        sa.city,

        -- Original FunnelFox origin (funnel path)
        sa.origin AS funnel_origin,

        -- Marketing Attribution
        sa.utm_source,
        sa.utm_medium,
        sa.utm_campaign,
        sa.utm_content,
        sa.utm_term,

        -- Click IDs (for joining with ad platform data)
        sa.fbclid,
        sa.gclid,
        sa.msclkid,
        sa.ttclid,

        -- Referrer
        sa.initial_referrer,
        sa.initial_referring_domain,

        -- Derived attribution
        sa.traffic_source_category,
        sa.is_paid_traffic,
        sa.ad_platform,

        -- Has attribution data
        CASE
            WHEN sa.utm_source IS NOT NULL OR sa.fbclid IS NOT NULL OR sa.gclid IS NOT NULL
                THEN TRUE
            ELSE FALSE
        END AS has_attribution

    FROM sessions_with_attribution sa
    LEFT JOIN funnels f
        ON sa.funnel_id = f.funnel_id
)

SELECT * FROM final
