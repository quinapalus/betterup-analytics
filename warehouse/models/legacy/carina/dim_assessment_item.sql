{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH member_satisfaction_assessment_items AS (

  SELECT * FROM {{ref('item_definition_program_satisfaction')}}

),

coach_satisfaction_assessment_items AS (

  SELECT * FROM {{ref('item_definition_coach_platform_satisfaction')}}

),

coach_observation_assessment_items AS (

  SELECT * FROM {{ref('item_definition_coach_observation')}}

),

member_whole_person_assessment_items AS (

  SELECT * FROM {{ref('item_definition_whole_person')}}

),

member_one_month_survey_items AS (

  SELECT * FROM {{ref('item_definition_one_month_survey')}}

),

member_post_session_assessment_items AS (

  SELECT * FROM {{ref('item_definition_post_session_member')}}
  -- focus only on the questions after the Mar 2019 product update
  WHERE item_key IN
    ('insight_rating',
      'session_overall_emotional',
      'session_was_valuable',
      'layer2_member_rated_goal_progress',
      'layer2_utility_rating',
      'covid_work_situation',
      'covid_well_being',
      'covid_disruptive',
      'covid_resource_beneficial',
      'feedback')

),

member_coach_matching_items AS (

  SELECT * FROM {{ref('item_definition_coach_matching')}}
  -- focus only on member motivation
  WHERE item_key IN ('motivation')

), 

final as (

  SELECT
    item_key AS assessment_item_key,
    'Whole Person Model 1.0' AS assessment_item_category,
    item_prompt AS assessment_item_prompt,
    scale AS assessment_item_response_scale
  FROM member_whole_person_assessment_items
  UNION ALL
  SELECT
    item_key AS assessment_item_key,
    'Member Satisfaction' AS assessment_item_category,
    item_prompt AS assessment_item_prompt,
    scale AS assessment_item_response_scale
  FROM member_satisfaction_assessment_items
  UNION ALL
  SELECT
    item_key AS assessment_item_key,
    'Post-Session Satisfaction' AS assessment_item_category,
    item_prompt AS assessment_item_prompt,
    scale AS assessment_item_response_scale
  FROM member_post_session_assessment_items
  UNION ALL
  SELECT
    item_key AS assessment_item_key,
    'One Month Survey' AS assessment_item_category,
    item_prompt AS assessment_item_prompt,
    scale AS assessment_item_response_scale
  FROM member_one_month_survey_items
  UNION ALL
  SELECT
    item_key AS assessment_item_key,
    'Member Onboarding' AS assessment_item_category,
    item_prompt AS assessment_item_prompt,
    scale AS assessment_item_response_scale
  FROM member_coach_matching_items
  UNION ALL
  SELECT
    item_key AS assessment_item_key,
    'Coach Satisfaction' AS assessment_item_category,
    item_prompt AS assessment_item_prompt,
    scale AS assessment_item_response_scale
  FROM coach_satisfaction_assessment_items
  UNION ALL
  SELECT
    item_key AS assessment_item_key,
    'Coach Observation' AS assessment_item_category,
    item_prompt AS assessment_item_prompt,
    scale AS assessment_item_response_scale
  FROM coach_observation_assessment_items
)

select {{ dbt_utils.surrogate_key(['assessment_item_key', 'assessment_item_category'])}} as dim_assessment_item_id 
  , *
from final