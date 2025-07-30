WITH assessment_items AS (

  SELECT * FROM {{ref('dei_assessment_items')}}
  -- pre-filter only relevant assessment types
  WHERE type IN
         ('Assessments::PostSessionMemberAssessment',
         'Assessments::PostSessionCoachAssessment',
         'Assessments::WholePersonProgramCheckinAssessment',
         'Assessments::WholePersonCoachFeedbackAssessment',
         'Assessments::OneMonthSurveyAssessment')
),

coach_matching_assessments AS (

  SELECT * FROM {{ref('dei_member_assessments')}}
  -- pre-filter only coach matching assessments from this model
  WHERE type = 'Assessments::CoachMatchingAssessment'

),

members AS (

  SELECT * FROM {{ref('dei_members')}}
  -- pre-filter only those members who submitted motivation
  WHERE motivation IS NOT NULL

),

member_engagement_by_day AS (

  SELECT * FROM {{ref('dei_member_engagement_by_day')}}

),

tracks AS (

  SELECT * FROM {{ref('dim_tracks')}} 
  WHERE is_external and engaged_member_count is not null --this logic was in dei_tracks which this model used to reference

),

dim_member AS (

  SELECT * FROM {{ref('dim_members')}}

),

dim_deployment AS (

  SELECT * FROM {{ref('dim_deployment')}}

),

dim_account AS (

  SELECT * FROM {{ref('dim_account')}}

),

dim_date AS (

  SELECT * FROM {{ref('dim_date')}}

),

post_session_assessment_items AS (

  SELECT
    user_id AS member_id,
    assessment_id,
    submitted_at,
    item_key AS assessment_item_key,
    'Post-Session Satisfaction' AS assessment_item_category,
    item_response AS assessment_item_response
  FROM assessment_items
  WHERE type = 'Assessments::PostSessionMemberAssessment'
    -- extract only text-based responses
    AND item_key IN ('feedback', 'covid_resource_beneficial')

),

one_month_survey_assessment_items AS (

  SELECT
    user_id AS member_id,
    assessment_id,
    submitted_at,
    item_key AS assessment_item_key,
    'One Month Survey'AS assessment_item_category,
    item_response AS assessment_item_response
  FROM assessment_items
  WHERE type = 'Assessments::OneMonthSurveyAssessment'
    -- extract only text-based responses
    AND item_key IN ('layer2_achievements', 'layer2_feedback')

),

program_satisfaction_assessment_items AS (

  SELECT
    user_id AS member_id,
    assessment_id,
    submitted_at,
    item_key AS assessment_item_key,
    'Member Satisfaction' AS assessment_item_category,
    item_response AS assessment_item_response
  FROM assessment_items
  WHERE type = 'Assessments::WholePersonProgramCheckinAssessment'
    -- extract only text-based responses
    AND item_key IN
      ('leveraging_strengths',
       'next_steps',
       'progress_toward_goals',
       'changed_or_grown',
       'milestones_achievements_or_learnings',
       'next_stage')

),

coach_observation_assessment_items AS (

  SELECT
    user_id AS member_id,
    assessment_id,
    submitted_at,
    item_key AS assessment_item_key,
    'Coach Observation' AS assessment_item_category,
    item_response AS assessment_item_response
  FROM assessment_items
  WHERE type = 'Assessments::WholePersonCoachFeedbackAssessment'
    -- extract only text-based responses
    AND item_key IN
      ('leveraging_strengths',
       'next_steps',
       'progress_toward_goals',
       'changed_or_grown',
       'milestones_achievements_or_learnings',
       'next_stage')

),

coach_observation_covid_assessment_items AS (

  SELECT
    user_id AS member_id,
    assessment_id,
    submitted_at,
    item_key AS assessment_item_key,
    'Coach Observation' AS assessment_item_category,
    item_response AS assessment_item_response
  FROM assessment_items
  WHERE type = 'Assessments::PostSessionCoachAssessment'
    -- extract only text-based responses
    AND item_key IN ('covid_session_helped')

),

sequenced_coach_matching_assessments AS (

  SELECT
    -- Identify the first coach matching assesssments
    -- whose dates of submission will be used to
    -- identify the date when a member provided their motivation.
    member_id,
    assessment_id,
    track_id,
    submitted_at,
    ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY member_id, submitted_at ASC) AS sequence
  FROM coach_matching_assessments
  QUALIFY sequence = 1

),

member_onboarding_assessment_items AS (

  SELECT
    -- join coach matching assessments to member
    -- motivations to mimic assessment item structure.
    -- Immediately join track information here.
    ca.member_id,
    ca.track_id,
    ca.assessment_id,
    t.organization_id,
    ca.submitted_at,
    'motivation' AS assessment_item_key,
    'Member Onboarding' AS assessment_item_category,
    m.motivation AS assessment_item_response
  FROM sequenced_coach_matching_assessments AS ca
  INNER JOIN members AS m
    ON ca.member_id = m.member_id
  INNER JOIN tracks AS t
    ON ca.track_id = t.track_id

),

qualitative_responses AS (

  SELECT
    ps.member_id,
    md.track_id,
    md.organization_id,
    ps.assessment_id,
    ps.submitted_at,
    ps.assessment_item_key,
    ps.assessment_item_category,
    ps.assessment_item_response
  FROM post_session_assessment_items AS ps
  INNER JOIN member_engagement_by_day AS md
    ON ps.member_id = md.member_id
    AND DATE_TRUNC('DAY', ps.submitted_at) = md.date_day

  UNION ALL

  SELECT
    os.member_id,
    md.track_id,
    md.organization_id,
    os.assessment_id,
    os.submitted_at,
    os.assessment_item_key,
    os.assessment_item_category,
    os.assessment_item_response
  FROM one_month_survey_assessment_items AS os
  INNER JOIN member_engagement_by_day AS md
    ON os.member_id = md.member_id
    AND DATE_TRUNC('DAY', os.submitted_at) = md.date_day

  UNION ALL

  SELECT
    pr.member_id,
    md.track_id,
    md.organization_id,
    pr.assessment_id,
    pr.submitted_at,
    pr.assessment_item_key,
    pr.assessment_item_category,
    pr.assessment_item_response
  FROM program_satisfaction_assessment_items AS pr
  INNER JOIN member_engagement_by_day AS md
    ON pr.member_id = md.member_id
    AND DATE_TRUNC('DAY', pr.submitted_at) = md.date_day

  UNION ALL

  SELECT
    co.member_id,
    md.track_id,
    md.organization_id,
    co.assessment_id,
    co.submitted_at,
    co.assessment_item_key,
    co.assessment_item_category,
    co.assessment_item_response
  FROM coach_observation_assessment_items AS co
  INNER JOIN member_engagement_by_day AS md
    ON co.member_id = md.member_id
    AND DATE_TRUNC('DAY', co.submitted_at) = md.date_day

  UNION ALL

  SELECT
    co.member_id,
    md.track_id,
    md.organization_id,
    co.assessment_id,
    co.submitted_at,
    co.assessment_item_key,
    co.assessment_item_category,
    co.assessment_item_response
  FROM coach_observation_covid_assessment_items AS co
  INNER JOIN member_engagement_by_day AS md
    ON co.member_id = md.member_id
    AND DATE_TRUNC('DAY', co.submitted_at) = md.date_day

  UNION ALL

  SELECT
    member_id,
    track_id,
    organization_id,
    assessment_id,
    submitted_at,
    assessment_item_key,
    assessment_item_category,
    assessment_item_response
  FROM member_onboarding_assessment_items

)


SELECT DISTINCT
  -- Surrogate Primary Key composed of member_key, deployment_key, account_key, date_key, assessment_item_key, and assessment_id
  {{ dbt_utils.surrogate_key(['dm.member_key', 'dp.deployment_key', 'da.account_key', 'dd.date_key', 'qr.assessment_item_key', 'qr.assessment_id']) }} AS id,
  dm.member_key,
  dp.deployment_key,
  -- diverge from convention here to brings keys through JOIN
  -- since we don't put app-specific IDs in dim_member_deployment.
  {{ member_deployment_key('qr.member_id', 'qr.track_id') }} AS member_deployment_key,
  da.account_key,
  dd.date_key,
  qr.assessment_item_key,
  qr.assessment_item_category,
  qr.assessment_item_response,
  {{ word_count('qr.assessment_item_response' )}} AS assessment_item_response_word_count,
  qr.assessment_id AS app_assessment_id
FROM qualitative_responses AS qr
INNER JOIN dim_member AS dm
  ON qr.member_id = dm.app_member_id
INNER JOIN dim_deployment AS dp
  ON qr.track_id = dp.app_track_id
INNER JOIN dim_account AS da
  ON qr.organization_id = da.app_organization_id
INNER JOIN dim_date AS dd
  ON {{ date_key('qr.submitted_at') }} = dd.date_key
