WITH reflection_points AS (

  SELECT * FROM {{ref('stg_app__reflection_points')}}

),

coach_assignments AS (

  SELECT * FROM {{ref('stg_app__coach_assignments')}}

),

track_assignments AS (

  SELECT * FROM {{ref('stg_app__track_assignments')}}

),

assessments AS (

  SELECT * FROM {{ref('int_app__assessments')}}

)


SELECT
  rp.reflection_point_id,
  ca.member_id,
  ca.coach_id,
  ta.track_id,
  ROW_NUMBER() OVER (PARTITION BY ca.member_id, ta.track_id ORDER BY rp.eligible_at)
    AS track_sequence_assigned_rp,

  -- pair boolean and success states for met_prereqs, coach_complete, and member_complete
  rp.met_prerequisites_at IS NOT NULL AS met_prereqs,
  CASE
    -- don't populate success state for backfilled objects
    WHEN rp.eligible_at < '12/01/2018' THEN NULL
    -- if member meets prereqs late, system will update eligible_at to roughly match met_prerequisites_at. Include small buffer here to account for latency
    WHEN dateadd('minute', 1, rp.met_prerequisites_at) < rp.eligible_at THEN 'success'
    WHEN rp.eligible_at > current_timestamp THEN 'pending'
    ELSE 'miss' -- either met late, or still not met
  END AS met_prereqs_success,

  rp.coach_assessment_id IS NOT NULL AS coach_complete,
  CASE
    WHEN rp.eligible_at < '12/01/2018' THEN NULL
    WHEN rp.met_prerequisites_at IS NULL THEN NULL
    WHEN rp.canceled_at IS NOT NULL THEN 'miss'
    WHEN rp.coach_assessment_id IS NOT NULL THEN 'success'
    ELSE 'pending'
  END AS coach_complete_success,

  rp.member_assessment_id IS NOT NULL AS member_complete,
  CASE
    WHEN rp.eligible_at < '12/01/2018' THEN NULL
    WHEN rp.coach_assessment_id IS NULL THEN NULL
    -- define 14 day "on-time" window for member to complete post coach completion
    WHEN COALESCE(mrp.submitted_at, current_timestamp) > 
        dateadd('day', 14, crp.submitted_at) THEN 'miss'
    WHEN rp.member_assessment_id IS NOT NULL THEN 'success'
    ELSE 'pending'
  END AS member_complete_success,

  rp.met_prerequisites_at,
  rp.upcoming_at,
  rp.eligible_at,
  rp.coach_due_at,
  rp.canceled_at,
  rp.coach_assessment_id,
  crp.submitted_at AS coach_submitted_at,
  rp.member_assessment_id,
  mrp.submitted_at AS member_submitted_at,
  rp.coach_assignment_id,
  rp.track_assignment_id,
  ca.role AS coach_type, 
  rp.created_at,
  rp.updated_at
FROM reflection_points AS rp
INNER JOIN coach_assignments AS ca ON rp.coach_assignment_id = ca.coach_assignment_id
INNER JOIN track_assignments AS ta ON rp.track_assignment_id = ta.track_assignment_id
LEFT OUTER JOIN assessments AS crp ON rp.coach_assessment_id = crp.assessment_id
LEFT OUTER JOIN assessments AS mrp ON rp.member_assessment_id = mrp.assessment_id
