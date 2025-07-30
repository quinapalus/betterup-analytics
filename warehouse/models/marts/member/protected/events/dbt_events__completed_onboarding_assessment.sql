WITH submitted_assessments AS (

  SELECT *
  FROM  {{ ref('dbt_events__submitted_assessment') }}

)


SELECT DISTINCT
  member_id,
  event_at,
  'completed' AS event_action,
  'onboarding_assessment' AS event_object,
  event_action || ' ' || event_object AS event_action_and_object,
  associated_record_type,
  associated_record_id,
  attributes
FROM submitted_assessments
WHERE attributes:"assessment_type"::VARCHAR IN (
      'Assessments::OnboardingAssessment',
      'Assessments::PrimaryCoachingModalitySetupAssessment',
      'Assessments::WholePersonAssessment'
      )
