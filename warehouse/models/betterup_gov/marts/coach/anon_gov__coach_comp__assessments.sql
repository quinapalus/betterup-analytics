WITH src_analytics_gov_coach_comp__assessments AS (
    SELECT * FROM {{ source('analytics_gov', 'coach_comp__assessments') }}

)

SELECT * FROM src_analytics_gov_coach_comp__assessments
