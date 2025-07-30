WITH src_analytics_eu_coach_comp__sessions AS (
    SELECT * FROM {{ source('analytics_eu', 'coach_comp__sessions') }}

)

SELECT * FROM src_analytics_eu_coach_comp__sessions
