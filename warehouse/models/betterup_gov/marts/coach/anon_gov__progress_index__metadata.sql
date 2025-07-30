WITH src_analytics_gov_progress_index__metadata AS (
    SELECT * FROM {{ source('analytics_gov', 'progress_index__metadata') }}

)

SELECT * FROM src_analytics_gov_progress_index__metadata
