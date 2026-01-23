{{
    config(
        materialized='view'
    )
}}

/*
    Staging model to transform raw_facebook_new.adsets
    into the same structure as raw_facebook.facebook_adsets
*/

WITH source AS (
    SELECT * FROM {{ source('raw_facebook_new', 'adsets') }}
),

final AS (
    SELECT
        -- Generate a unique ID
        ROW_NUMBER() OVER (ORDER BY id) AS id,

        -- Source identifier
        'facebook_new' AS source,

        -- IDs with facebook_ prefix for compatibility
        id AS facebook_adset_id,
        campaign_id AS facebook_campaign_id,
        account_id AS facebook_account_id,

        -- Adset details
        name AS adset_name,
        status,
        effective_status,

        -- Budget fields (convert from text to numeric)
        NULLIF(daily_budget::numeric, 0) / 100.0 AS daily_budget,  -- FB stores in cents
        NULLIF(lifetime_budget::numeric, 0) / 100.0 AS lifetime_budget,  -- FB stores in cents

        -- Budget type (inferred from which budget is set)
        CASE
            WHEN daily_budget IS NOT NULL AND daily_budget != '' THEN 'daily'
            WHEN lifetime_budget IS NOT NULL AND lifetime_budget != '' THEN 'lifetime'
            ELSE NULL
        END AS budget_type,

        -- Pacing type (extract from jsonb)
        CASE
            WHEN pacing_type IS NOT NULL THEN pacing_type::text
            ELSE NULL
        END AS pacing_type,

        -- Target countries (extract from targeting jsonb if available)
        CASE
            WHEN targeting IS NOT NULL AND targeting->'geo_locations'->'countries' IS NOT NULL
            THEN (targeting->'geo_locations'->'countries')::text
            ELSE NULL
        END AS target_countries,

        -- Timestamps (convert from text to timestamp)
        created_time::timestamp AS created_time,
        updated_time::timestamp AS updated_time,

        -- Audit timestamps
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at

    FROM source
    WHERE id IS NOT NULL
)

SELECT * FROM final
