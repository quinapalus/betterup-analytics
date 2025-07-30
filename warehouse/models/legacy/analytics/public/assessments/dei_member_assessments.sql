WITH assessments AS (

  SELECT * FROM {{ref('int_app__assessments')}}

),

track_assignments AS (

  SELECT * FROM {{ref('dbt_track_assignments')}}

),

submitted_member_assessments AS (

  SELECT
    *
  FROM assessments
  WHERE submitted_at IS NOT NULL
    -- filter out assessments that are exclusively non-member:
    AND type NOT IN ('Assessments::CoachProficiencyAssessment')

)


SELECT
  sm.assessment_id,
  sm.user_id AS member_id,
  ta.track_id,
  sm.creator_id,
  sm.type,
  sm.questions_version,
  sm.created_at,
  sm.submitted_at,
  CASE
    -- populate report_generated_at using submitted_at for Onboarding and Reflection Point assessments
    WHEN sm.type IN ('Assessments::WholePersonAssessment', 'Assessments::WholePersonProgramCheckinAssessment', 'Assessments::WholePersonGroupCoachingCheckinAssessment') THEN sm.submitted_at
    ELSE sm.report_generated_at
  END AS report_generated_at
FROM submitted_member_assessments AS sm
INNER JOIN track_assignments AS ta
  -- Infer association to track based on dates of invite and closure.
  ON sm.user_id = ta.member_id
  AND sm.submitted_at > ta.created_at
  AND (ta.ended_at IS NULL OR sm.submitted_at < dateadd('hour', 12, ta.ended_at))
-- Remove any duplicate rows introduced in track assignment join:
QUALIFY ROW_NUMBER() OVER
  (PARTITION BY sm.assessment_id ORDER BY ta.created_at DESC) = 1
