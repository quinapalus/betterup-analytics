WITH reflection_points AS (

  SELECT * FROM {{ ref('stg_app__reflection_points') }}

),

coach_assignments AS (

  SELECT * FROM {{ ref('stg_app__coach_assignments') }}

),

assessments AS (

  SELECT * FROM {{ ref('stg_app__assessments') }}

)


SELECT
  rp.reflection_point_id,
  rp.coach_assignment_id,
  ca.coach_id,
  ca.member_id,
  rp.met_prerequisites_at,
  rp.upcoming_at,
  rp.eligible_at,
  rp.coach_due_at,
  rp.canceled_at,
  rp.coach_assessment_id,
  crp.submitted_at AS coach_assessment_submitted_at,
  rp.member_assessment_id,
  mrp.submitted_at AS member_assessment_submitted_at,
  rp.created_at,
  rp.updated_at
FROM reflection_points AS rp
INNER JOIN coach_assignments AS ca
  ON rp.coach_assignment_id = ca.coach_assignment_id
LEFT OUTER JOIN assessments AS crp
  ON rp.coach_assessment_id = crp.assessment_id
LEFT OUTER JOIN assessments AS mrp
  ON rp.member_assessment_id = mrp.assessment_id
