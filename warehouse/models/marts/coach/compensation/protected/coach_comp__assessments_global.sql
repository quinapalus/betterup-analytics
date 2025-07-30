{{ config(
    tags=["eu"],
    schema="coach"
) }}

WITH
coach_comp__assessments_us AS (
    SELECT * FROM {{ ref('coach_comp__assessments') }}
),
coach_comp__assessments_eu AS (
    SELECT * FROM {{ source('analytics_eu_read_only_anon', 'anon_eu__coach_comp__assessments') }}
),
coach_comp__assessments_unioned AS (
    SELECT * FROM coach_comp__assessments_us
    UNION ALL
    SELECT * FROM coach_comp__assessments_eu
),
coach_comp__assessments_global AS (
    SELECT
        user_season_id
        , user_uuid
        , season_name
        , SUM(lifetime_assessments) AS lifetime_assessments_global
        , SUM(primary_b2b_assessments) AS primary_b2b_assessments_global
        , SUM(lifetime_lca_assessments) AS lifetime_lca_assessments_global
        , SUM(primary_b2b_lca_assessments) AS primary_b2b_lca_assessments_global
    FROM coach_comp__assessments_unioned
    GROUP BY 1, 2, 3
)

SELECT * FROM coach_comp__assessments_global
