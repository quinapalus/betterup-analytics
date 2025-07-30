WITH track_assignments AS (

  SELECT * FROM {{ ref('stg_app__track_assignments') }}
  WHERE NOT is_hidden

),

submitted_assessments AS (

  SELECT *
  FROM  {{ ref('dbt_events__submitted_assessment') }}

),

submitted_onboarding_assessments AS (

  SELECT DISTINCT
    member_id,
    event_at as submitted_onboarding_assessment_at
  FROM submitted_assessments
  WHERE attributes:"assessment_type"::VARCHAR IN (
        'Assessments::OnboardingAssessment',
        'Assessments::PrimaryCoachingModalitySetupAssessment',
        'Assessments::WholePersonAssessment'
        )

),

track_assignments_enriched AS (
  SELECT
    ta.track_assignment_id,
    ta.member_id,
    ta.track_id,
    ta.created_at,
    ta.ended_at,
    ROW_NUMBER() OVER (PARTITION BY ta.member_id, ta.track_id ORDER BY ta.created_at) AS sequence,
    MIN(o.submitted_onboarding_assessment_at) OVER (PARTITION BY ta.member_id, ta.track_id) AS completed_onboarding_assessment_at,
    -- a member is considered onboarded on the track if they completed
    -- an onboarding assessment within any associated track assignment
    BOOLOR_AGG(o.submitted_onboarding_assessment_at IS NOT NULL) OVER (PARTITION BY ta.member_id, ta.track_id) AS member_completed_onboarding_assessment,
    -- a member is considered open if any one of their assignments is open, that is, not ended
    BOOLOR_AGG(ta.ended_at IS NULL) OVER (PARTITION BY ta.member_id, ta.track_id) AS member_is_open
  FROM track_assignments AS ta
  LEFT OUTER JOIN submitted_onboarding_assessments AS o
    -- join in any completed_onboarding assessment that happened prior to ta.ended_at
    ON
      ta.member_id = o.member_id AND
      (ta.ended_at IS NULL OR o.submitted_onboarding_assessment_at < ta.ended_at)

)


SELECT
  -- Surrogate Key of the Member ID + Track Assignment ID
  {{ dbt_utils.surrogate_key(['member_id', 'track_assignment_id']) }} AS member_track_assignment_id,
  
  {# /* 
  -- Updated Version for when updated to dbt_utils >= 1.0.0
  -- {{ dbt_utils.generate_surrogate_key(['member_id','track_assignment_id']) }} AS member_track_assignment_id,
  */ #}
  
  member_id,
  -- if member completed onboarding prior to track invite, mark as onboarded on invite
  GREATEST(completed_onboarding_assessment_at, created_at) AS event_at,
  'completed' AS event_action,
  'onboarding_assessment_track' AS event_object,
  event_action || ' ' || event_object AS event_action_and_object,
  'TrackAssignment' AS associated_record_type,
  track_assignment_id AS associated_record_id,
  OBJECT_CONSTRUCT('member_is_open', member_is_open, 'completed_member_onboarding_assessment_prior_to_invitation', completed_onboarding_assessment_at < created_at) AS attributes
FROM track_assignments_enriched
WHERE member_completed_onboarding_assessment AND sequence = 1