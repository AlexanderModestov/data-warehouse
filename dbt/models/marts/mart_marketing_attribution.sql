{{
    config(
        materialized='table'
    )
}}

/*
    Marketing Attribution Mart

    Purpose: Session-level marketing attribution combining FunnelFox sessions,
             Stripe payments, Amplitude events, and Facebook campaigns.
    Grain: One row per session (includes all sessions, with or without payments)

    Data Flow:
    1. FunnelFox Sessions (base) -> LEFT JOIN Stripe Subscriptions & Charges
    2. LEFT JOIN Amplitude (device_id = 'fnlfx_' + profile_id) for UTM attribution
    3. LEFT JOIN Facebook campaigns for campaign metadata

    Payment Filters (when joined):
    - Only successful charges (status = 'succeeded')
    - Not refunded (refunded IS FALSE)
    - Subscription creation or invoice payments only

    IMPORTANT: Revenue is sourced from actual Stripe charges (not FunnelFox prices)
    to ensure consistency with mart_new_subscriptions and accurate ROAS calculations.
*/

-- =============================================================================
-- 1. BASE: All Sessions with optional payment data (LEFT JOINs)
-- =============================================================================

/*
    PAYMENT LINKAGE STRATEGY:

    Session -> Subscription (via ff_session_id) -> Charge (via time proximity)

    We link subscriptions to their first successful charge using:
    1. Same customer (subscription.customer = charge.customer)
    2. Charge created within 120 seconds of subscription
    3. Charge description = 'Subscription creation'
    4. Charge status = 'succeeded' and not refunded

    Revenue = actual charge amount (NOT plan amount)
    This ensures we only count revenue for payments that actually succeeded.
*/

-- Match subscriptions to their first successful charge via time proximity
WITH subscription_charges AS (
    SELECT DISTINCT ON (subs.id)
        subs.id AS subscription_id,
        subs.metadata->>'ff_session_id' AS ff_session_id,
        subs.customer,
        subs.status AS subscription_status,
        subs.created AS subscription_created_at,
        subs.plan->>'interval' AS billing_interval,
        (subs.plan->>'interval_count')::int AS billing_interval_count,
        subs.plan->>'currency' AS currency,
        ch.id AS charge_id,
        ch.amount / 100.0 AS charge_amount_usd
    FROM {{ source('raw_stripe', 'subscriptions') }} subs
    INNER JOIN {{ source('raw_stripe', 'charges') }} ch
        ON ch.customer = subs.customer
        AND ch.status = 'succeeded'
        AND ch.refunded = FALSE
        AND ch.description = 'Subscription creation'
        AND ABS(EXTRACT(EPOCH FROM (ch.created - subs.created))) < 120
    WHERE subs.metadata->>'ff_session_id' IS NOT NULL
    ORDER BY subs.id, ABS(EXTRACT(EPOCH FROM (ch.created - subs.created)))
),

session_payments AS (
    SELECT
        -- Session data
        fnlfx_ss.id AS session_id,
        fnlfx_ss.profile_id,
        fnlfx_ss.funnel_id,
        fnlfx_ss.created_at AS session_created_at,
        fnlfx_ss.country,
        fnlfx_ss.city,
        fnlfx_ss.origin,
        fnlfx_ss.user_agent,
        fnlfx_ss.ip,
        fnlfx_ss.funnel_version,

        -- Stripe subscription data
        COALESCE(sc.subscription_id, subs.id) AS stripe_subscription_id,
        COALESCE(sc.subscription_status, subs.status) AS stripe_subscription_status,
        COALESCE(sc.subscription_created_at, subs.created) AS stripe_subscription_created_at,
        COALESCE(sc.customer, subs.customer) AS stripe_customer_id,

        -- Charge data
        sc.charge_id AS stripe_charge_id,

        -- Revenue from actual charge (only if > $2 to exclude test transactions)
        -- Only assign revenue to FIRST session per charge to avoid double-counting
        CASE
            WHEN sc.charge_amount_usd > 2.0
                 AND ROW_NUMBER() OVER (
                     PARTITION BY sc.charge_id
                     ORDER BY fnlfx_ss.created_at ASC
                 ) = 1
            THEN sc.charge_amount_usd
            ELSE NULL
        END AS revenue_usd,

        -- Subscription plan details
        COALESCE(sc.billing_interval, subs.plan->>'interval') AS billing_interval,
        COALESCE(sc.billing_interval_count, (subs.plan->>'interval_count')::int) AS billing_interval_count,
        COALESCE(sc.currency, subs.plan->>'currency') AS currency,

        -- FunnelFox subscription data (for additional info)
        fs.id AS funnelfox_subscription_id,
        fs.status AS funnelfox_subscription_status,
        fs.payment_provider,
        fs.created_at AS funnelfox_subscription_created_at,

        -- Funnel metadata
        fnl.title AS funnel_title,
        fnl.type AS funnel_type,
        fnl.environment AS funnel_environment,
        fnl.alias AS funnel_alias

    FROM {{ source('raw_funnelfox', 'sessions') }} fnlfx_ss

    -- LEFT JOIN funnel metadata
    LEFT JOIN {{ source('raw_funnelfox', 'funnels') }} fnl
        ON fnlfx_ss.funnel_id = fnl.id

    -- LEFT JOIN Stripe subscriptions via ff_session_id in metadata
    LEFT JOIN {{ source('raw_stripe', 'subscriptions') }} subs
        ON subs.metadata->>'ff_session_id' = fnlfx_ss.id

    -- LEFT JOIN subscription charges (matched via time proximity)
    LEFT JOIN subscription_charges sc
        ON sc.ff_session_id = fnlfx_ss.id

    -- LEFT JOIN FunnelFox subscriptions for billing info
    LEFT JOIN {{ source('raw_funnelfox', 'subscriptions') }} fs
        ON COALESCE(sc.subscription_id, subs.id) = fs.psp_id
        AND fs.sandbox = FALSE
),

-- =============================================================================
-- 4a. AMPLITUDE: Extract profile_id from device_id and get first-touch attribution
-- =============================================================================
amplitude_attribution AS (
    SELECT DISTINCT ON (profile_id)
        -- Extract profile_id from device_id (format: 'fnlfx_{profile_id}')
        SUBSTRING(device_id FROM 7) AS profile_id,

        -- First-touch UTM attribution from user_properties
        user_properties->>'initial_utm_source' AS utm_source,
        user_properties->>'initial_utm_medium' AS utm_medium,
        user_properties->>'initial_utm_campaign' AS utm_campaign,
        user_properties->>'initial_utm_content' AS utm_content,
        user_properties->>'initial_utm_term' AS utm_term,
        user_properties->>'initial_fbclid' AS fbclid,
        user_properties->>'initial_gclid' AS gclid,
        user_properties->>'initial_referrer' AS referrer,
        user_properties->>'initial_referring_domain' AS referring_domain,

        -- Amplitude identifiers
        device_id AS amplitude_device_id,
        amplitude_id,
        platform AS amplitude_platform,
        os_name AS amplitude_os,

        MIN(event_time) AS first_amplitude_event_at

    FROM {{ source('raw_amplitude', 'events') }}
    WHERE device_id LIKE 'fnlfx_%'
    GROUP BY
        SUBSTRING(device_id FROM 7),
        user_properties->>'initial_utm_source',
        user_properties->>'initial_utm_medium',
        user_properties->>'initial_utm_campaign',
        user_properties->>'initial_utm_content',
        user_properties->>'initial_utm_term',
        user_properties->>'initial_fbclid',
        user_properties->>'initial_gclid',
        user_properties->>'initial_referrer',
        user_properties->>'initial_referring_domain',
        device_id,
        amplitude_id,
        platform,
        os_name
    ORDER BY profile_id, MIN(event_time)
),

-- =============================================================================
-- 4b. FACEBOOK: Campaign lookup tables
-- =============================================================================
-- Primary: Match by campaign_id
facebook_campaigns_by_id AS (
    SELECT DISTINCT ON (facebook_campaign_id)
        facebook_campaign_id,
        campaign_name,
        objective AS campaign_objective
    FROM {{ source('raw_facebook', 'facebook_campaigns') }}
    ORDER BY facebook_campaign_id, created_time DESC
),

-- Secondary: Match by campaign_name (fallback)
-- Note: utm_campaign values are URL-encoded (+ for spaces, %2C for commas)
facebook_campaigns_by_name AS (
    SELECT DISTINCT ON (campaign_name)
        facebook_campaign_id,
        campaign_name,
        -- Create URL-encoded version for matching (spaces -> +, commas -> %2C)
        REPLACE(REPLACE(campaign_name, ' ', '+'), ',', '%2C') AS campaign_name_encoded,
        objective AS campaign_objective
    FROM {{ source('raw_facebook', 'facebook_campaigns') }}
    WHERE campaign_name IS NOT NULL
    ORDER BY campaign_name, created_time DESC
),

-- =============================================================================
-- FINAL: Join everything together
-- =============================================================================
final AS (
    SELECT
        -- User identifier
        sp.profile_id,

        -- Session info
        sp.session_id,
        sp.session_created_at,
        DATE(sp.session_created_at AT TIME ZONE 'UTC') AS session_date,

        -- Funnel context
        sp.funnel_id,
        sp.funnel_title,
        sp.funnel_type,
        sp.funnel_environment,
        sp.funnel_alias,
        sp.funnel_version,
        sp.origin,

        -- Geography
        sp.country,
        sp.city,

        -- UTM Attribution (from Amplitude first-touch)
        amp.utm_source,
        amp.utm_medium,
        amp.utm_campaign,
        amp.utm_content,
        amp.utm_term,
        amp.fbclid,
        amp.gclid,
        amp.referrer,
        amp.referring_domain,

        -- Facebook campaign matching
        COALESCE(fb_by_id.facebook_campaign_id, fb_by_name.facebook_campaign_id) AS facebook_campaign_id,
        COALESCE(fb_by_id.campaign_name, fb_by_name.campaign_name) AS facebook_campaign_name,
        COALESCE(fb_by_id.campaign_objective, fb_by_name.campaign_objective) AS facebook_campaign_objective,

        -- Amplitude metadata
        amp.amplitude_device_id,
        amp.amplitude_id,
        amp.amplitude_platform,
        amp.amplitude_os,
        amp.first_amplitude_event_at,

        -- Stripe subscription & charge data
        sp.stripe_subscription_id,
        sp.stripe_subscription_status,
        sp.stripe_customer_id,
        sp.stripe_subscription_created_at,
        sp.stripe_charge_id,

        -- FunnelFox subscription data
        sp.funnelfox_subscription_id,
        sp.funnelfox_subscription_status,
        sp.payment_provider,

        -- Billing details (from Stripe plan)
        sp.billing_interval,
        sp.billing_interval_count,
        sp.currency,

        -- Revenue (from subscription plan, only if payment succeeded)
        sp.revenue_usd,

        -- Time to convert (subscription time - session time)
        EXTRACT(EPOCH FROM (sp.stripe_subscription_created_at - sp.session_created_at)) / 3600.0 AS hours_to_convert,

        -- Attribution channel classification
        CASE
            WHEN LOWER(amp.utm_source) IN ('facebook', 'fb') AND LOWER(amp.utm_medium) IN ('cpc', 'paid', 'ppc', 'paidsocial', 'paid_social')
                THEN 'facebook_paid'
            WHEN LOWER(amp.utm_source) IN ('facebook', 'fb')
                THEN 'facebook_organic'
            WHEN amp.fbclid IS NOT NULL
                THEN 'facebook_paid'
            WHEN LOWER(amp.utm_source) IN ('google') AND LOWER(amp.utm_medium) IN ('cpc', 'paid', 'ppc')
                THEN 'google_paid'
            WHEN LOWER(amp.utm_source) = 'google'
                THEN 'google_organic'
            WHEN amp.gclid IS NOT NULL
                THEN 'google_paid'
            WHEN LOWER(amp.utm_medium) IN ('cpc', 'paid', 'ppc')
                THEN 'other_paid'
            WHEN amp.utm_source IS NOT NULL
                THEN 'other_' || LOWER(amp.utm_source)
            ELSE 'direct'
        END AS attribution_channel,

        -- Paid traffic flag
        CASE
            WHEN LOWER(amp.utm_medium) IN ('cpc', 'paid', 'ppc', 'paidsocial', 'paid_social')
                OR amp.fbclid IS NOT NULL
                OR amp.gclid IS NOT NULL
            THEN TRUE
            ELSE FALSE
        END AS is_paid_traffic,

        -- Facebook traffic flag
        CASE
            WHEN LOWER(amp.utm_source) IN ('facebook', 'fb')
                OR amp.fbclid IS NOT NULL
            THEN TRUE
            ELSE FALSE
        END AS is_facebook_traffic

    FROM session_payments sp

    -- Amplitude attribution (profile_id matched via 'fnlfx_' prefix)
    LEFT JOIN amplitude_attribution amp
        ON sp.profile_id = amp.profile_id

    -- Facebook campaigns: primary match by campaign_id
    LEFT JOIN facebook_campaigns_by_id fb_by_id
        ON amp.utm_campaign = fb_by_id.facebook_campaign_id

    -- Facebook campaigns: secondary match by campaign_name (fallback)
    -- Match against URL-encoded campaign name (+ for spaces, %2C for commas)
    LEFT JOIN facebook_campaigns_by_name fb_by_name
        ON amp.utm_campaign = fb_by_name.campaign_name_encoded
        AND fb_by_id.facebook_campaign_id IS NULL
)

SELECT * FROM final
