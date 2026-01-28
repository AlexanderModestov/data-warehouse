{{
    config(
        materialized='table'
    )
}}

/*
    Staging model to transform raw_facebook_new.adsinsights_default
    into the same structure as raw_facebook.facebook_ad_statistics

    This enables switching from the old Facebook extractor to the new one
    while maintaining compatibility with existing downstream models.
*/

WITH source AS (
    SELECT * FROM {{ source('raw_facebook_new', 'adsinsights_default') }}
),

-- Extract values from actions array for specific action types
actions_extracted AS (
    SELECT
        s.*,
        -- Purchases (use single action type - all purchase types are duplicates with same value)
        (
            SELECT COALESCE(SUM((elem->>'value')::numeric), 0)
            FROM unnest(s.actions) AS elem
            WHERE elem->>'action_type' = 'purchase'
        ) AS purchases_extracted,

        -- Leads (use single action type - all lead types are duplicates with same value)
        (
            SELECT COALESCE(SUM((elem->>'value')::numeric), 0)
            FROM unnest(s.actions) AS elem
            WHERE elem->>'action_type' = 'lead'
        ) AS leads_extracted,

        -- Registrations (use single action type - all registration types are duplicates with same value)
        (
            SELECT COALESCE(SUM((elem->>'value')::numeric), 0)
            FROM unnest(s.actions) AS elem
            WHERE elem->>'action_type' = 'complete_registration'
        ) AS registrations_extracted,

        -- Video 3-sec plays
        (
            SELECT COALESCE(SUM((elem->>'value')::numeric), 0)
            FROM unnest(s.video_play_actions) AS elem
        ) AS video_3_sec_plays_extracted,

        -- Thru-plays (video watched to completion or 15+ sec)
        (
            SELECT COALESCE(SUM((elem->>'value')::numeric), 0)
            FROM unnest(s.video_15_sec_watched_actions) AS elem
        ) AS thru_plays_extracted

    FROM source s
),

-- Extract cost per action values
costs_extracted AS (
    SELECT
        a.*,
        -- Cost per purchase
        (
            SELECT (elem->>'value')::numeric
            FROM unnest(a.cost_per_action_type) AS elem
            WHERE elem->>'action_type' IN ('purchase', 'omni_purchase', 'offsite_conversion.fb_pixel_purchase')
            LIMIT 1
        ) AS cost_per_purchase_extracted,

        -- Cost per lead
        (
            SELECT (elem->>'value')::numeric
            FROM unnest(a.cost_per_action_type) AS elem
            WHERE elem->>'action_type' IN ('lead', 'offsite_conversion.fb_pixel_lead')
            LIMIT 1
        ) AS cost_per_lead_extracted,

        -- Cost per registration
        (
            SELECT (elem->>'value')::numeric
            FROM unnest(a.cost_per_action_type) AS elem
            WHERE elem->>'action_type' IN ('complete_registration', 'omni_complete_registration')
            LIMIT 1
        ) AS cost_per_registration_extracted,

        -- Cost per thruplay
        (
            SELECT (elem->>'value')::numeric
            FROM unnest(a.cost_per_thruplay) AS elem
            LIMIT 1
        ) AS cost_per_thruplay_extracted

    FROM actions_extracted a
),

final AS (
    SELECT
        -- Generate a unique ID (row_number based on composite key)
        ROW_NUMBER() OVER (ORDER BY date_start, ad_id) AS id,

        -- Source identifier
        'facebook_new' AS source,

        -- IDs with facebook_ prefix for compatibility
        ad_id AS facebook_ad_id,
        adset_id AS facebook_adset_id,
        campaign_id AS facebook_campaign_id,
        account_id AS facebook_account_id,

        -- Date fields
        date_start::date AS report_date,
        date_start::date AS reporting_start,
        date_stop::date AS reporting_end,

        -- Names
        account_name,
        campaign_name,
        adset_name,
        ad_name,

        -- Core metrics (cast from text to proper types)
        COALESCE(impressions::bigint, 0) AS impressions,
        COALESCE(clicks::bigint, 0) AS clicks,
        COALESCE(unique_clicks::bigint, 0) AS unique_clicks,
        COALESCE(spend::numeric, 0) AS amount_spent,

        -- Calculated metrics
        NULLIF(cpc::numeric, 0) AS cpc,
        NULLIF(cpm::numeric, 0) AS cpm,
        NULLIF(cost_per_unique_click::numeric, 0) AS cost_per_unique_click,
        NULLIF(ctr::numeric, 0) AS ctr,

        -- Video metrics (calculated rates)
        CASE
            WHEN COALESCE(impressions::bigint, 0) > 0
            THEN video_3_sec_plays_extracted::numeric / impressions::bigint
            ELSE NULL
        END AS hook_rate,
        CASE
            WHEN COALESCE(video_3_sec_plays_extracted, 0) > 0
            THEN thru_plays_extracted::numeric / video_3_sec_plays_extracted
            ELSE NULL
        END AS hold_rate,
        COALESCE(video_3_sec_plays_extracted::bigint, 0) AS video_3_sec_plays,
        COALESCE(thru_plays_extracted::bigint, 0) AS thru_plays,

        -- Conversion metrics
        COALESCE(leads_extracted::bigint, 0) AS leads,
        cost_per_lead_extracted AS cost_per_lead,
        COALESCE(registrations_extracted::bigint, 0) AS registrations_completed,
        cost_per_registration_extracted AS cost_per_registration,
        COALESCE(purchases_extracted::bigint, 0) AS purchases,
        cost_per_purchase_extracted AS cost_per_purchase,

        -- Budget fields (not available in insights, set to NULL)
        NULL::numeric AS adset_budget,
        NULL::text AS adset_budget_type,

        -- Delivery status (not directly available)
        NULL::text AS ad_delivery,

        -- Timestamps
        CURRENT_TIMESTAMP AS data_fetched_at,

        -- Targeting (not available in insights)
        NULL::text AS targeting_countries,

        -- Audit timestamps
        created_time::timestamp AS created_at,
        updated_time::timestamp AS updated_at

    FROM costs_extracted
    WHERE date_start IS NOT NULL
)

SELECT * FROM final
