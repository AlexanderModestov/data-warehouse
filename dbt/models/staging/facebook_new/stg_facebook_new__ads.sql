{{
    config(
        materialized='view'
    )
}}

/*
    Staging model to transform raw_facebook_new.ads
    into the same structure as raw_facebook.facebook_ads
*/

WITH source AS (
    SELECT * FROM {{ source('raw_facebook_new', 'ads') }}
),

final AS (
    SELECT
        -- Generate a unique ID
        ROW_NUMBER() OVER (ORDER BY id) AS id,

        -- Source identifier
        'facebook_new' AS source,

        -- IDs with facebook_ prefix for compatibility
        id AS facebook_ad_id,
        adset_id AS facebook_adset_id,
        campaign_id AS facebook_campaign_id,
        account_id AS facebook_account_id,

        -- Ad details
        name AS ad_name,
        status,
        effective_status,

        -- Delivery status (not directly available in new schema)
        NULL::text AS ad_delivery_status,

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
