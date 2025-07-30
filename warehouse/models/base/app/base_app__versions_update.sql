{{
    config(
        materialized='incremental',
        on_schema_change='fail'
    )
}}

WITH versions_update AS (
    SELECT *
    FROM {{ ref('base_app__versions') }}
    WHERE event = 'update'
)

SELECT *
FROM versions_update
{% if is_incremental() %}
  WHERE created_at > (SELECT max(created_at) FROM {{ this }})
{% endif %}