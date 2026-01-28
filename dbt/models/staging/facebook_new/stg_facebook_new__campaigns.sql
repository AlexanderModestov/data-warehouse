{{
    config(
        materialized='table'
    )
}}

/*
    Staging model to transform raw_facebook_new.campaigns
    into the same structure as raw_facebook.facebook_campaigns
*/

WITH source AS (
    SELECT * FROM {{ source('raw_facebook_new', 'campaigns') }}
),

final AS (
    SELECT
        -- Generate a unique ID
        ROW_NUMBER() OVER (ORDER BY id) AS id,

        -- Source identifier
        'facebook_new' AS source,

        -- IDs with facebook_ prefix for compatibility
        id AS facebook_campaign_id,
        account_id AS facebook_account_id,

        -- Campaign details
        name AS campaign_name,
        objective,
        status,
        effective_status,

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
