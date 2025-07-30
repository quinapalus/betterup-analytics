WITH src_analytics_gov_coach_comp__5in90_sessions AS (
    SELECT * FROM {{ source('analytics_gov', 'coach_comp__5in90_sessions') }}

)

SELECT * FROM src_analytics_gov_coach_comp__5in90_sessions