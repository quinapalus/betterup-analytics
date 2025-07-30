WITH src_analytics_eu_coach_comp__assessments AS (
    SELECT * FROM {{ source('analytics_eu', 'coach_comp__assessments') }}

)

SELECT * FROM src_analytics_eu_coach_comp__assessments
