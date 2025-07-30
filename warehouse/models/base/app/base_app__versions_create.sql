{{
    config(
        materialized='incremental',
        on_schema_change='fail'
    )
}}

WITH versions_create AS (
    SELECT *
    FROM {{ ref('base_app__versions') }}
    WHERE event = 'create'
)

SELECT *
FROM versions_create
{% if is_incremental() %}
  WHERE created_at > (SELECT max(created_at) FROM {{ this }})
{% endif %}