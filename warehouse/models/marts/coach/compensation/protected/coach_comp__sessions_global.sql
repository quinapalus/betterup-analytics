{{ config(
    tags=["eu"],
    schema="coach"
) }}

WITH
coach_comp__sessions_us AS (
    SELECT * FROM {{ ref('coach_comp__sessions') }}
),
coach_comp__sessions_eu AS (
    SELECT * FROM {{ source('analytics_eu_read_only_anon', 'anon_eu__coach_comp__sessions') }}
),
coach_comp__sessions_unioned AS (
    SELECT * FROM coach_comp__sessions_us
    UNION ALL
    SELECT * FROM coach_comp__sessions_eu
),
coach_comp__sessions_global AS (
    SELECT
        user_season_id
        , user_uuid
        , season_name
        , SUM(lifetime_sessions) AS lifetime_sessions_global
        , SUM(primary_b2b_lifetime_sessions) AS primary_b2b_lifetime_sessions_global
    FROM coach_comp__sessions_unioned
    GROUP BY 1, 2, 3
)

SELECT * FROM coach_comp__sessions_global
