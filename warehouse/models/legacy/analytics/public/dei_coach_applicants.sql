{{
  config(
    tags=['classification.c3_confidential'],
    materialized='view'
  )
}}

WITH applicants AS (
  SELECT * FROM {{ref('stg_fountain__applicants')}}
),

priority_languages AS (
  SELECT * FROM {{ref('bu_priority_staffing_languages')}}
),

languages_by_applicant AS (
  SELECT * FROM (
    SELECT
      sc.fountain_applicant_id,
      sc.coaching_languages AS staffing_languages,
      pl.language AS ranked_language,
      pl.priority_rank,
      ROW_NUMBER() OVER (
          PARTITION BY sc.fountain_applicant_id
          ORDER BY sc.fountain_applicant_id, pl.priority_rank ASC NULLS LAST
      ) AS index
    FROM applicants AS sc
    -- join if any of a applicant's languages are in the priority staffing list
    LEFT OUTER JOIN priority_languages AS pl
      ON ARRAY_CONTAINS(pl.language::variant, sc.coaching_languages) = true
    -- for each applicant, select the language with the highest priority_rank
    ) a
  WHERE index = 1
),

time_zone_geo_categories AS (
  SELECT * FROM {{ref('int_csv__time_zone_geo_categories')}}
),

transitions AS (
  SELECT * FROM {{ref('stg_fountain__transitions')}}
),

coaches AS (
  SELECT * FROM {{ref('dei_coaches')}}
),

transition_to_s2 AS (
  -- defining start of "Stage 2: Review" as first transition to any Background
  -- Check stage. Note this relies on name matching to Stage name in Fountain.

  SELECT
    fountain_applicant_id,
    MIN(created_at) AS transitioned_at
  FROM transitions
  WHERE stage_name LIKE 'Background Check%'
  GROUP BY fountain_applicant_id
),

transition_to_s4 AS (

  -- defining start of "Stage 4: Finalize" as first transition to stage named
  -- "Hold Until MT 2.0 Complete". Note this relies on exact name matching to
  -- Stage name in Fountain.

  SELECT
    fountain_applicant_id,
    MIN(created_at) AS transitioned_at
  FROM transitions
  WHERE stage_name LIKE 'Hold Until MT 2.0 Complete'
  GROUP BY fountain_applicant_id

),

wpm2_mock_sessions AS (

  SELECT * FROM {{ref('dbt_wpm2_mock_sessions')}}

)

SELECT * FROM (

SELECT
  -- Fountain data isn't unique on email so we're selecting the Fountain account
  -- with most recent activity only
  a.fountain_applicant_id,
  c.coach_id,
  a.primary_email AS email,
  a.first_name,
  a.last_name,
  a.secondary_email,
  a.time_zone,
  tzg.tz_iana,
  a.country,
  tzg.country_code,
  tzg.country_name,
  tzg.subregion_m49,
  a.receive_automated_emails,
  a.inspiring_matrix,
  a.inspiring_application_tier,
  a.thriving_matrix,
  a.thriving_application_tier,
  a.coach_interview_tier,
  a.vip_status,
  a.high_priority,
  COALESCE(c.geo, tzg.geo) AS geo,
  {{ priority_language('pl.ranked_language', 'pl.staffing_languages') }} AS priority_language,
  pl.staffing_languages,
  c.coaching_cloud,
  CASE
    WHEN a.stage = 'Rejected' THEN 'rejected'
    WHEN c.first_staffable_at IS NOT NULL OR c.staffable_state <> 'onboarding' THEN 'hired'
    WHEN s4.transitioned_at IS NOT NULL THEN 's4-finalize'
    WHEN c.created_at IS NOT NULL THEN 's3-train'
    WHEN s2.transitioned_at IS NOT NULL THEN 's2-contract'
    ELSE 's1-review'
  END AS stage_name,
  CASE
    WHEN a.last_transitioned_at > dateadd('day', -60, current_timestamp) THEN 'last-60'
    WHEN a.last_transitioned_at > dateadd('day', -90, current_timestamp) THEN 'last-90'
    ELSE 'last-90+'
  END AS fountain_active,
  a.funnel AS fountain_funnel,
  a.stage AS fountain_stage,
  a.created_at AS s1_review_started_at,
  s2.transitioned_at AS s2_contract_started_at,
  c.created_at AS s3_train_started_at,
  s4.transitioned_at AS s4_finalize_started_at,
  c.staffable_state,
  c.first_staffable_at AS staffed_at,
  a.last_transitioned_at,
  row_number() over (
      PARTITION BY a.primary_email
      ORDER BY a.primary_email, a.last_transitioned_at DESC
  ) as index
FROM applicants AS a
LEFT OUTER JOIN languages_by_applicant AS pl
  ON a.fountain_applicant_id = pl.fountain_applicant_id
LEFT OUTER JOIN transition_to_s2 AS s2
  ON a.fountain_applicant_id = s2.fountain_applicant_id
LEFT OUTER JOIN transition_to_s4 AS s4
  ON a.fountain_applicant_id = s4.fountain_applicant_id
LEFT OUTER JOIN coaches AS c
  ON a.primary_email = c.email
LEFT OUTER JOIN time_zone_geo_categories AS tzg
  ON a.time_zone = tzg.time_zone
LEFT OUTER JOIN wpm2_mock_sessions
  ON c.email = wpm2_mock_sessions.coach_email

) a

WHERE index = 1
