WITH src_analytics_eu_coach_comp__5in90_sessions AS (
    SELECT * FROM {{ source('analytics_eu', 'coach_comp__5in90_sessions') }}

)

SELECT * FROM src_analytics_eu_coach_comp__5in90_sessions