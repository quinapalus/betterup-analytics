{{
    config(
        materialized='incremental',
        on_schema_change='fail'
    )
}}

WITH versions_destroy AS (
    SELECT *
    FROM {{ ref('base_app__versions') }}
    WHERE event = 'destroy'
        AND item_type NOT IN ('Notification')
)

SELECT *
FROM versions_destroy
{% if is_incremental() %}
  WHERE created_at > (SELECT max(created_at) FROM {{ this }})
{% endif %}