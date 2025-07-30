{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH assessments AS (

  SELECT * FROM {{ref('int_app__assessments')}}

),

sessions AS (

  SELECT * FROM {{ref('dei_sessions')}}

),

reflection_points AS (

  SELECT * FROM {{ref('dei_reflection_points')}}

),

coach_assignments AS (

  SELECT * FROM {{ref('stg_app__coach_assignments')}}

),

assessment_item_response AS (

  SELECT * FROM {{ref('dei_assessment_items')}}

),

dim_assessment_item AS (

  SELECT * FROM {{ref('dim_assessment_item')}}
  -- exclude text-based responses
  WHERE assessment_item_response_scale <> 'text_response'

),

dim_member AS (

  SELECT * FROM {{ref('dim_members')}}

),

dim_account AS (

  SELECT * FROM {{ref('dim_account')}}

),

coach_key AS (

  SELECT * FROM {{ref('dbt_coach_key')}}

),

member_engagement_by_day AS (

  SELECT * FROM {{ref('dei_member_engagement_by_day')}}

),

member_engagement_track_metrics AS (

  SELECT * FROM {{ref('dei_member_engagement_track_metrics')}}

),

coach_type_responses AS (

  SELECT
    a.assessment_id,
    s.coach_id,
    s.coach_type
  FROM assessments AS a
  INNER JOIN sessions AS s
    ON a.responses:appointment_id::INT = s.session_id
  WHERE a.type = 'Assessments::PostSessionMemberAssessment'

  UNION ALL

  (SELECT assessment_id, coach_id, coach_type FROM

  (SELECT
    a.assessment_id,
    -- note: some RP assessments don't have matching reflection_point records
    -- select coach from reflection_points, or fall back to first primary coach assignment
    COALESCE(r.coach_id, ca.coach_id) AS coach_id,
    -- assume primary coach for RP if no matching assessment in reflection_points
    COALESCE(r.coach_type, 'primary') AS coach_type,
    -- sanitize any historical overlapping coach_assignments
    ROW_NUMBER() OVER (
        PARTITION BY a.assessment_id
        ORDER BY a.assessment_id, ca.created_at
    ) AS index
  FROM assessments AS a
  LEFT OUTER JOIN reflection_points AS r
    ON a.assessment_id = r.member_assessment_id
  LEFT OUTER JOIN coach_assignments AS ca
    ON a.user_id = ca.member_id AND
       ca.role = 'primary' AND
       a.submitted_at > ca.created_at AND
       (ca.ended_at IS NULL OR a.submitted_at < ca.ended_at)
  WHERE a.type = 'Assessments::WholePersonProgramCheckinAssessment'
  ) a

  WHERE index = 1)

  UNION ALL

  SELECT
    a.assessment_id,
    m.primary_coach_id AS coach_id,
    'primary' AS coach_type
  FROM assessments AS a
  INNER JOIN member_engagement_by_day AS m
    ON a.user_id = m.member_id
    AND DATE_TRUNC('DAY', a.submitted_at) = m.date_day
  WHERE a.type = 'Assessments::OneMonthSurveyAssessment'

)


SELECT
  {{ date_key('r.submitted_at') }} AS date_key,
  {{ member_key('r.user_id') }} AS member_key,
  ck.coach_key,
  ck.app_coach_id,
  {{ account_key('m.organization_id', 'm.sfdc_account_id') }} AS account_key,
  {{ deployment_key('m.track_id') }} AS deployment_key,
  {{ member_deployment_key('r.user_id', 'm.track_id') }} AS member_deployment_key,
  i.assessment_item_key,
  i.assessment_item_category,
  CASE
    WHEN i.assessment_item_key IN ('net_promoter', 'employer_nps') THEN {{ score_nps('r.item_response::int') }}
    WHEN i.assessment_item_key = 'session_overall_emotional' THEN {{ score_session_overall ('r.item_response') }}
    WHEN i.assessment_item_key IN ('covid_work_situation') THEN NULL -- NULL for the unique COVID-19 question that offers multiple-choice answers
    ELSE r.item_response::int
  END AS assessment_item_score,
  r.item_response AS member_response,
  CASE
    WHEN i.assessment_item_key IN ('net_promoter', 'employer_nps') THEN
      {{ sanitize_nps_response_category('r.item_response') }}
    WHEN i.assessment_item_response_scale = '1-Strongly Disagree;2-Disagree;3-Neither Disagree nor Agree;4-Agree;5-Strongly Agree' THEN
      {{ sanitize_agreement_response_category('r.item_response') }}
  END AS member_response_category,
  {{ get_date_difference ('me.invite_date', 'r.submitted_at') }} AS days_since_invite,
  {{ get_date_difference ('me.activation_date', 'r.submitted_at') }} AS days_since_activation,
  m.completed_session_count_track_to_date,
  c.coach_type,
  r.assessment_id AS app_assessment_id,
  COUNT(*) OVER (PARTITION BY m.organization_id, i.assessment_item_key) AS account_item_n
FROM assessment_item_response AS r
INNER JOIN dim_assessment_item AS i
  ON
    CASE
      WHEN r.type = 'Assessments::WholePersonProgramCheckinAssessment' THEN i.assessment_item_category = 'Member Satisfaction'
      WHEN r.type = 'Assessments::PostSessionMemberAssessment' THEN i.assessment_item_category = 'Post-Session Satisfaction'
      WHEN r.type = 'Assessments::OneMonthSurveyAssessment' THEN i.assessment_item_category = 'One Month Survey'
    END
   AND r.item_key = i.assessment_item_key
INNER JOIN member_engagement_by_day AS m
  ON r.user_id = m.member_id AND {{ date_key('r.submitted_at') }} = {{ date_key('m.date_day') }} AND
     m.is_external
INNER JOIN member_engagement_track_metrics AS me
  ON m.member_id = me.member_id
  AND m.track_id = me.track_id
INNER JOIN coach_type_responses AS c
  ON r.assessment_id = c.assessment_id
INNER JOIN coach_key AS ck
  ON c.coach_id = ck.app_coach_id
WHERE
  -- filter for RP assessments
  (r.type IN ('Assessments::WholePersonProgramCheckinAssessment') OR
  -- filter for post-session member assessment after Mar 2019 product change
  (r.type IN ('Assessments::PostSessionMemberAssessment') AND r.submitted_at > '03/08/2019') OR
  -- filter for one-month pulse assessment
  (r.type IN ('Assessments::OneMonthSurveyAssessment'))) AND
  -- ensure foreign keys are present in dimension tables
  {{ member_key('r.user_id') }} IN (SELECT member_key FROM dim_member) AND
  {{ account_key('m.organization_id', 'm.sfdc_account_id') }} IN (SELECT account_key FROM dim_account)
