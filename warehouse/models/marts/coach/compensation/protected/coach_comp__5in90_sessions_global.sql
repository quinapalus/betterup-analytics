{{ config(
    tags=["eu"],
    schema="coach"
) }}

WITH
coach_comp__5in90_us AS (
    SELECT * FROM {{ ref('coach_comp__5in90_sessions') }}
),
coach_comp__5in90_eu AS (
    SELECT * FROM {{ source('analytics_eu_read_only_anon', 'anon_eu__coach_comp__5in90_sessions') }}
),
coach_comp__5in90_unioned AS (
    SELECT * FROM coach_comp__5in90_us
    UNION ALL
    SELECT * FROM coach_comp__5in90_eu
),
coach_comp__5in90_global AS (
    SELECT
        user_season_id
        , user_uuid
        , season_name
        , SUM(primary_b2b_first_time_sessions) AS primary_b2b_first_time_sessions_global
        , SUM(primary_b2b_5_in_90_sessions) AS primary_b2b_5_in_90_sessions_global
    FROM coach_comp__5in90_unioned
    GROUP BY 1, 2, 3
)

select * from coach_comp__5in90_global
