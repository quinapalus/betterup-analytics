WITH src_analytics_eu_progress_index__metadata AS (
    SELECT * FROM {{ source('analytics_eu', 'progress_index__metadata') }}

)

SELECT * FROM src_analytics_eu_progress_index__metadata
