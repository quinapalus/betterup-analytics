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
    SELECT * FROM {{ ref('coach_comp__assessments_global') }}
),
coach_comp_sessions AS (
    SELECT * FROM {{ ref('coach_comp__sessions_global') }}
),
coach_comp_5in90_sessions AS (
    SELECT * FROM {{ ref('coach_comp__5in90_sessions_global') }}
),
coach_profile AS (
    SELECT * FROM {{ ref('int_coach__coach_profiles') }}
),
impact_metrics AS (

  SELECT
    se.user_season_id
    , c.user_uuid
    , c.coach_id AS us_coach_id
    , c.coach_profile_uuid
    , c.full_name
    , s.season_name

--    sessions
    , se.lifetime_sessions_global
    , se.primary_b2b_lifetime_sessions_global

--    assessments
    , a.lifetime_assessments_global
    , a.primary_b2b_assessments_global

--    lca assessments (life-changing/amazing assessments)
    , a.lifetime_lca_assessments_global
    , a.primary_b2b_lca_assessments_global

--    lca rates
    , FLOOR(a.lifetime_lca_assessments_global / NULLIFZERO(a.lifetime_assessments_global) * 1.00, 2) AS lca_rate_global --rounded down to the nearest %
    , FLOOR(a.primary_b2b_lca_assessments_global / NULLIFZERO(a.primary_b2b_assessments_global) * 1.00, 2) AS primary_b2b_lca_rate_global --rounded down to the nearest %

--    5 in 90 rates
    , f.primary_b2b_first_time_sessions_global
    , f.primary_b2b_5_in_90_sessions_global
    , FLOOR(f.primary_b2b_5_in_90_sessions_global / NULLIFZERO(f.primary_b2b_first_time_sessions_global) * 1.00, 2) AS primary_b2b_5_in_90_ratio_global

  FROM coach_profile c
  LEFT JOIN seasons s

  LEFT JOIN coach_comp_sessions se
    ON se.user_uuid = c.user_uuid
    AND se.season_name = s.season_name
  LEFT JOIN coach_comp_assessments a
    ON a.user_uuid = c.user_uuid
    AND a.season_name = s.season_name
  LEFT JOIN coach_comp_5in90_sessions f
    ON f.user_uuid = c.user_uuid
    AND f.season_name = s.season_name

WHERE c.staffable_state IN ('staffable','hold_involuntary','hold_voluntary')
)

SELECT *
FROM impact_metrics
ORDER BY 1,2,3,4
