{{ config(
    tags=["eu"],
    schema="coach"
) }}

WITH
seasons AS (
    SELECT * FROM {{ ref('dim_coach_compensation_seasons') }}
),
dim_coaches AS (
    SELECT * FROM {{ ref('dim_coaches') }}
),
coach_comp_assessments AS (
    SELECT * FROM {{ ref('coach_comp__assessments') }}
),
coach_comp_sessions AS (
    SELECT * FROM {{ ref('coach_comp__sessions') }}
),
coach_comp_5in90_sessions AS (
    SELECT * FROM {{ ref('coach_comp__5in90_sessions') }}
),
impact_metrics AS (

  SELECT
    c.coach_id
    , c.user_uuid
    , c.coach_profile_uuid
    , c.full_name
    , s.season_name

--    sessions
    , se.lifetime_sessions
    , se.primary_b2b_lifetime_sessions

--    assessments
    , a.lifetime_assessments
    , a.primary_b2b_assessments

--    lca assessments (life-changing/amazing assessments)
    , a.lifetime_lca_assessments
    , a.primary_b2b_lca_assessments

--    lca rates
    , FLOOR(a.lifetime_lca_assessments / NULLIFZERO(a.lifetime_assessments) * 1.00, 2) AS lca_rate --rounded down to the nearest %
    , FLOOR(a.primary_b2b_lca_assessments / NULLIFZERO(a.primary_b2b_assessments) * 1.00, 2) AS primary_b2b_lca_rate --rounded down to the nearest %

--    5 in 90 rates
    , f.primary_b2b_first_time_sessions
    , f.primary_b2b_5_in_90_sessions
    , FLOOR(f.primary_b2b_5_in_90_sessions / NULLIFZERO(f.primary_b2b_first_time_sessions) * 1.00, 2) AS primary_b2b_5_in_90_ratio

  FROM dim_coaches c
  LEFT JOIN seasons s

  LEFT JOIN coach_comp_sessions se
    ON se.coach_id = c.coach_id -- this will change to s.coach_profile_uuid = c.coach_profile_uuid
    AND se.season_name = s.season_name
  LEFT JOIN coach_comp_assessments a
    ON a.coach_id = c.coach_id -- this will change to s.coach_profile_uuid = c.coach_profile_uuid
    AND a.season_name = s.season_name
  LEFT JOIN coach_comp_5in90_sessions f
        ON f.coach_id = c.coach_id -- this will change to s.coach_profile_uuid = c.coach_profile_uuid
    AND f.season_name = s.season_name

WHERE c.staffable_state IN ('staffable','hold_involuntary','hold_voluntary')
)

SELECT
    {{ dbt_utils.surrogate_key(['user_uuid','season_name']) }} AS user_season_id
    , impact_metrics.*
FROM impact_metrics
ORDER BY 1,2,3,4