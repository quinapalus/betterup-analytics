{{
  config(
    tags=['classification.c3_confidential','eu'],
    materialized='table'
  )
}}

WITH engagement_events AS (
  SELECT * FROM {{ref('stg_app__engagement_events')}}
),

assessments AS (
  SELECT * FROM {{ref('int_app__assessments')}}
),

activities AS (
  SELECT * FROM {{ref('stg_app__activities')}}
),

resources AS (
  SELECT * FROM {{ref('stg_app__resources')}}
),

coach_assignments AS (
  SELECT * FROM {{ref('stg_app__coach_assignments')}}
),

completed_member_assessments AS (
  SELECT
    e.user_id AS member_id,
    e.eventable_id,
    e.eventable_type,
    e.verb,
    e.event_at,
    'Completed Assessments' AS event_category,
    LEAST(DATEDIFF('SECOND', a.created_at, a.submitted_at)::FLOAT/60, 60) AS duration_minutes -- cap maximum duration at one hour for assessments
  FROM assessments AS a
  INNER JOIN engagement_events AS e
    ON a.assessment_id = e.eventable_id
  WHERE
    (e.eventable_type, e.verb) = ('Assessment', 'submitted') 
    AND a.submitted_at IS NOT NULL 
    -- filter for member-completed relevant assessments
    AND ( a.type IN
          ('Assessments::WholePersonProgramCheckinAssessment', 'Assessments::PostSessionMemberAssessment', 'Assessments::WholePersonAssessment',
           'Assessments::MemberNpsAssessment', 'Assessments::WholePerson360Assessment', 'Assessments::WholePerson180Assessment', 'Assessments::PostGroupCoachingSessionAssessment',
           'Assessments::PreGroupCoachingCohortAssessment', 'Assessments::WorkshopPostSessionSurveyAssessment', 'Assessments::OneMonthSurveyAssessment',
           'Assessments::WholePersonGroupCoachingCheckinAssessment'
          )
      or a.assessment_configuration_id is not null
    )
),

completed_member_activities AS (
  SELECT distinct
    e.user_id AS member_id,
    e.eventable_id,
    e.eventable_type,
    e.verb,
    e.event_at,
    'Coach-Assigned Activities' AS event_category,
    r.duration AS duration_minutes
  FROM activities AS a
  INNER JOIN resources AS r
    ON a.resource_id = r.resource_id
    AND r.type IN ('Resources::ActionItemResource', 'Resources::ActionableResource', 'Resources::ContentResource', 'Resources::EdsResource', 'Resources::LinkResource', 'Resources::SpecialistCoachResource', 'Resources::NonPrimaryCoachingJourneyResource', 'Resources::InteractiveTestResource', 'Resources::DeepLinkResource', 'Resources::IngeniuxResource', 'Resources::ExtendedNetworkFaqResource', 'GainClarityExperimentResource') -- do not pull in assessments, only coach-assigned activities & action items
  INNER JOIN engagement_events AS e 
    ON a.activity_id = e.eventable_id
  WHERE
    (e.eventable_type, e.verb) = ('Activity', 'completed') 
    AND a.completed_at IS NOT NULL 
    AND a.creator_id != a.member_id -- ensure these are coach-assigned activities
    AND a.creator_id IN (SELECT distinct coach_id FROM coach_assignments)
),

member_activities_messages_goals AS (
  SELECT distinct --it is possible for duplicate message_sent events to be logged for the same message, so we dedupe here with this distinct statement
    user_id AS member_id,
    eventable_id,
    eventable_type,
    verb,
    event_at,
    'Messages and Goals' AS event_category,
    -- filter only for those activities that we have to estimate length
    CASE
      WHEN (eventable_type, verb) = ('Message', 'sent') OR (eventable_type, verb) = ('GroupCoachingCohort', 'sent') OR (eventable_type, verb) = ('Resource', 'shared') OR (eventable_type, verb) = ('RatedResource', 'created') THEN 1
      WHEN (eventable_type, verb) = ('Objective', 'completed') OR (eventable_type, verb) = ('Objective', 'shared') OR (eventable_type, verb) = ('Objective', 'edited') OR (eventable_type, verb) = ('Objective', 'created') OR (eventable_type, verb) = ('ObjectiveCheckIn', 'completed') OR (eventable_type, verb) = ('Resource', 'viewed') THEN 2
      ELSE NULL
    END AS duration_minutes
  FROM engagement_events
  WHERE
    (eventable_type, verb) = ('Message', 'sent') OR
    (eventable_type, verb) = ('Objective', 'completed') OR
    (eventable_type, verb) = ('Objective', 'shared') OR
    (eventable_type, verb) = ('Objective', 'edited') OR
    (eventable_type, verb) = ('Objective', 'created') OR
    (eventable_type, verb) = ('ObjectiveCheckIn', 'completed') OR
    (eventable_type, verb) = ('GroupCoachingCohort', 'sent')
),

member_activities_resources AS (
  SELECT distinct
    e.user_id AS member_id,
    e.eventable_id,
    e.eventable_type,
    e.verb,
    MIN(e.event_at) OVER(PARTITION BY e.user_id, e.eventable_id, e.eventable_type, e.verb) as event_at,
    'Resources' AS event_category,
    CASE WHEN (e.eventable_type, e.verb) = ('Resource', 'viewed') THEN r.duration
         WHEN (e.eventable_type, e.verb) = ('Resource', 'shared') OR (e.eventable_type, e.verb) = ('RatedResource', 'created') THEN 1
         ELSE NULL
    END AS duration_minutes
  FROM engagement_events AS e
  INNER JOIN resources AS r
    ON e.eventable_id = r.resource_id
  WHERE
    --filter out resources that were assigned to the user as an activity
    e.eventable_id NOT IN (SELECT distinct resource_id FROM activities a WHERE e.user_id = a.member_id) 
    AND ((e.eventable_type, e.verb) = ('Resource', 'shared') 
    OR (e.eventable_type, e.verb) = ('RatedResource', 'created') 
    OR (e.eventable_type, e.verb) = ('Resource', 'viewed'))
),

unioned as (
  SELECT * FROM completed_member_assessments union all
  SELECT * FROM completed_member_activities union all
  SELECT * FROM member_activities_messages_goals union all
  SELECT * FROM member_activities_resources
),

final as (

  select {{ dbt_utils.surrogate_key(['member_id','eventable_id','eventable_type','verb','event_at'])}} as non_session_event_time_id,
    * 
  from unioned

)

select * from final
