WITH src_analytics_gov_coach_comp__sessions AS (
    SELECT * FROM {{ source('analytics_gov', 'coach_comp__sessions') }}

)

SELECT * FROM src_analytics_gov_coach_comp__sessions
