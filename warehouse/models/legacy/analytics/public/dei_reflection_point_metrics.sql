WITH member_engagement_by_day AS (

  SELECT * FROM {{ref('dei_member_engagement_by_day')}}

),

reflection_points AS (

  SELECT * FROM {{ref('dei_reflection_points')}}

),

member_assessments AS (

  SELECT * FROM {{ref('dei_member_assessments')}}

),

accounts AS (

  SELECT * FROM {{ref('dei_accounts')}}

),

tracks AS (

  SELECT * FROM {{ref('dim_tracks')}} 
  WHERE is_external and engaged_member_count is not null --this logic was in dei_tracks which this model used to reference

),

member_track_assessments_sequenced AS (

  -- sequence baseline and RP assessments so that later we can
  -- choose only those member-track pairs for which the
  -- onboarding assessment was the first assessment. Note that
  -- we filter for only RP and onboarding types, otherwise
  -- the sequencing-based comparison doesn't work due to other
  -- assessments being included.

  SELECT
    member_id,
    track_id,
    submitted_at,
    type,
    ROW_NUMBER() OVER (PARTITION BY member_id, track_id ORDER BY submitted_at) AS assessment_sequence
  FROM member_assessments
  WHERE type IN ('Assessments::WholePersonAssessment', 'Assessments::WholePersonProgramCheckinAssessment')

),

member_track_with_rp AS (

  SELECT DISTINCT
    md.member_id,
    md.track_id,
    a.organization_id,
    t.reflection_point_interval_days,
    t.reflection_point_interval_appointments,
    t.deployment_type
  FROM member_engagement_by_day AS md
  INNER JOIN tracks AS t
    ON md.track_id = t.track_id
    -- filter for tracks that have RPs assigned
    AND t.num_reflection_points > 0
  INNER JOIN accounts AS a
    ON t.organization_id = a.organization_id

),

member_track_onboarding AS (

  SELECT
    member_id,
    track_id,
    submitted_at
  FROM member_track_assessments_sequenced
  WHERE type = 'Assessments::WholePersonAssessment'
  -- choose those onboarding assessments that were
  -- the first assessment for the member-track pair.
  AND assessment_sequence = 1

),

met_prerequisites_earliest AS (

  SELECT * FROM (

  SELECT
    member_id,
    track_id,
    coach_id,
    met_prerequisites_at,
    eligible_at,
    ROW_NUMBER() OVER (
        PARTITION BY member_id, track_id
        ORDER BY member_id, track_id, met_prerequisites_at
    ) AS index
  FROM reflection_points
  WHERE met_prerequisites_at IS NOT NULL
  
  ) a
  
  WHERE index = 1
  

),

met_prerequisites_latest AS (

  SELECT * FROM (

  SELECT
    member_id,
    track_id,
    coach_id,
    met_prerequisites_at,
    eligible_at,
    ROW_NUMBER() OVER (
        PARTITION BY member_id, track_id
        ORDER BY member_id, track_id, met_prerequisites_at DESC
    ) AS index
  FROM reflection_points
  WHERE met_prerequisites_at IS NOT NULL
  
  ) a
  
  WHERE index = 1

),

coach_completed_earliest AS (

  SELECT * FROM (

  SELECT
    member_id,
    track_id,
    coach_id,
    coach_submitted_at,
    ROW_NUMBER() OVER (
        PARTITION BY member_id, track_id
        ORDER BY member_id, track_id, coach_submitted_at
    ) AS index
  FROM reflection_points
  WHERE coach_submitted_at IS NOT NULL
   
  ) a
  
  WHERE index = 1

),

coach_completed_latest AS (

  SELECT * FROM (

  SELECT
    member_id,
    track_id,
    coach_id,
    coach_submitted_at,
    ROW_NUMBER() OVER (
        PARTITION BY member_id, track_id
        ORDER BY member_id, track_id, coach_submitted_at DESC
    ) as index
  FROM reflection_points
  WHERE coach_submitted_at IS NOT NULL
  
  ) a

  WHERE index = 1

),

member_completed_earliest AS (

  SELECT * FROM (

  SELECT
    member_id,
    track_id,
    coach_id,
    member_submitted_at,
    ROW_NUMBER() OVER (
        PARTITION BY member_id, track_id
        ORDER BY member_id, track_id, member_submitted_at
    ) AS index
  FROM reflection_points
  WHERE member_submitted_at IS NOT NULL
  
  ) a
  
  WHERE index = 1

),

member_completed_latest AS (

  SELECT * FROM (

  SELECT
    member_id,
    track_id,
    coach_id,
    member_submitted_at,
    ROW_NUMBER() OVER (
        PARTITION BY member_id, track_id
        ORDER BY member_id, track_id, member_submitted_at DESC
    ) AS index
  FROM reflection_points
  WHERE member_submitted_at IS NOT NULL
  
  ) a
  
  WHERE index = 1

),

member_track_rp_stats AS (

  SELECT
    member_id,
    track_id,
    COUNT(coach_submitted_at) AS coach_submitted_count,
    COUNT(member_submitted_at) AS member_submitted_count,
    COUNT(canceled_at) AS rp_canceled_count
  FROM reflection_points
  GROUP BY member_id, track_id

)


SELECT
  mt.member_id,
  mt.track_id,
  mt.organization_id,
  mo.submitted_at AS baseline_assessment_completed_at,
  -- earliest prerequisites block
  pe.met_prerequisites_at IS NOT NULL AS earliest_met_prerequisites,
  pe.met_prerequisites_at AS earliest_met_prerequisites_at,
  pe.eligible_at AS earliest_eligible_at,
  {{ get_date_difference('mo.submitted_at', 'pe.met_prerequisites_at') }} AS baseline_days_before_earliest_prerequisites,
  pe.coach_id AS earliest_met_prerequisites_coach_id,
  mdpe.completed_session_count_track_to_date AS earliest_met_prerequisites_session_count,
  mdee.completed_session_count_track_to_date AS earliest_eligible_session_count,
  -- latest prerequisites block
  pl.met_prerequisites_at IS NOT NULL AS latest_met_prerequisites,
  pl.met_prerequisites_at AS latest_met_prerequisites_at,
  pl.eligible_at AS latest_eligible_at,
  {{ get_date_difference('mo.submitted_at', 'pl.met_prerequisites_at') }} AS baseline_days_before_latest_prerequisites,
  pl.coach_id AS latest_met_prerequisites_coach_id,
  mdpl.completed_session_count_track_to_date AS latest_met_prerequisites_session_count,
  mdle.completed_session_count_track_to_date AS latest_eligible_session_count,
  -- earliest coach block
  ce.coach_submitted_at IS NOT NULL AS earliest_coach_submitted,
  ce.coach_submitted_at AS earliest_coach_submitted_at,
  {{ get_date_difference('mo.submitted_at', 'ce.coach_submitted_at') }} AS baseline_days_before_earliest_coach,
  {{ get_date_difference('pe.eligible_at', 'ce.coach_submitted_at') }} AS eligible_days_before_earliest_coach,
  ce.coach_id AS earliest_submitted_coach_id,
  mdce.completed_session_count_track_to_date AS earliest_coach_submitted_session_count,
  -- latest coach block
  cl.coach_submitted_at IS NOT NULL AS latest_coach_submitted,
  cl.coach_submitted_at AS latest_coach_submitted_at,
  {{ get_date_difference('mo.submitted_at', 'cl.coach_submitted_at') }} AS baseline_days_before_latest_coach,
  {{ get_date_difference('pl.eligible_at', 'cl.coach_submitted_at') }} AS eligible_days_before_latest_coach,
  cl.coach_id AS latest_submitted_coach_id,
  mdcl.completed_session_count_track_to_date AS latest_coach_submitted_session_count,
  -- earliest member block
  me.member_submitted_at IS NOT NULL AS earliest_member_submitted,
  me.member_submitted_at AS earliest_member_submitted_at,
  {{ get_date_difference('mo.submitted_at', 'me.member_submitted_at') }} AS baseline_days_before_earliest_member,
  me.coach_id AS earliest_member_submitted_coach_id,
  mdme.completed_session_count_track_to_date AS earliest_member_submitted_session_count,
  -- latest member block
  ml.member_submitted_at IS NOT NULL AS latest_member_submitted,
  ml.member_submitted_at AS latest_member_submitted_at,
  {{ get_date_difference('mo.submitted_at', 'ml.member_submitted_at') }} AS baseline_days_before_latest_member,
  ml.coach_id AS latest_member_submitted_coach_id,
  mdml.completed_session_count_track_to_date AS latest_member_submitted_session_count,
  -- RP stats block
  rs.coach_submitted_count,
  rs.member_submitted_count,
  rs.rp_canceled_count,
  -- track information block
  mt.reflection_point_interval_days,
  mt.reflection_point_interval_appointments,
  mt.deployment_type
FROM member_track_with_rp AS mt
-- filter for those member-track pair combinations
-- that had onboarding assessment completed using INNER JOIN.
INNER JOIN member_track_onboarding AS mo
  ON mt.member_id = mo.member_id
  AND mt.track_id = mo.track_id
LEFT OUTER JOIN met_prerequisites_earliest AS pe
  ON mt.member_id = pe.member_id
  AND mt.track_id = pe.track_id
LEFT OUTER JOIN member_engagement_by_day AS mdpe
  -- extract number of sessions for earliest met prerequisites
  ON pe.member_id = mdpe.member_id
  AND pe.track_id = mdpe.track_id
  AND (pe.met_prerequisites_at IS NOT NULL AND DATE_TRUNC('DAY', pe.met_prerequisites_at) = mdpe.date_day)
LEFT OUTER JOIN member_engagement_by_day AS mdee
-- extract number of sessions for earliest eligibility
  ON pe.member_id = mdee.member_id
  AND pe.track_id  = mdee.track_id
  AND (pe.met_prerequisites_at IS NOT NULL AND DATE_TRUNC('DAY', pe.eligible_at) = mdee.date_day)
LEFT OUTER JOIN met_prerequisites_latest AS pl
  ON mt.member_id = pl.member_id
  AND mt.track_id = pl.track_id
LEFT OUTER JOIN member_engagement_by_day AS mdpl
  -- extract number of sessions for latest met prerequisites
  ON pl.member_id = mdpl.member_id
  AND pl.track_id = mdpl.track_id
  AND (pl.met_prerequisites_at IS NOT NULL AND DATE_TRUNC('DAY', pl.met_prerequisites_at) = mdpl.date_day)
LEFT OUTER JOIN member_engagement_by_day AS mdle
-- extract number of sessions for latest eligibility
  ON pl.member_id = mdle.member_id
  AND pl.track_id  = mdle.track_id
  AND (pl.met_prerequisites_at IS NOT NULL AND DATE_TRUNC('DAY', pl.eligible_at) = mdle.date_day)
LEFT OUTER JOIN coach_completed_earliest AS ce
  ON mt.member_id = ce.member_id
  AND mt.track_id = ce.track_id
LEFT OUTER JOIN member_engagement_by_day AS mdce
  -- extract number of sessions for earliest coach submission
  ON ce.member_id = mdce.member_id
  AND ce.track_id = mdce.track_id
  AND (ce.coach_submitted_at IS NOT NULL AND DATE_TRUNC('DAY', ce.coach_submitted_at) = mdce.date_day)
LEFT OUTER JOIN coach_completed_latest AS cl
  ON mt.member_id = cl.member_id
  AND mt.track_id = cl.track_id
LEFT OUTER JOIN member_engagement_by_day AS mdcl
  -- extract number of sessions for latest coach submission
  ON cl.member_id = mdcl.member_id
  AND cl.track_id = mdcl.track_id
  AND (cl.coach_submitted_at IS NOT NULL AND DATE_TRUNC('DAY', cl.coach_submitted_at) = mdcl.date_day)
LEFT OUTER JOIN member_completed_earliest AS me
  ON mt.member_id = me.member_id
  AND mt.track_id = me.track_id
LEFT OUTER JOIN member_engagement_by_day AS mdme
  -- extract number of sessions for earliest member submission
  ON me.member_id = mdme.member_id
  AND me.track_id = mdme.track_id
  AND (me.member_submitted_at IS NOT NULL AND DATE_TRUNC('DAY', me.member_submitted_at) = mdme.date_day)
LEFT OUTER JOIN member_completed_latest AS ml
  ON mt.member_id = ml.member_id
  AND mt.track_id = ml.track_id
LEFT OUTER JOIN member_engagement_by_day AS mdml
  -- extract number of sessions for latest member submission
  ON ml.member_id = mdml.member_id
  AND ml.track_id = mdml.track_id
  AND (ml.member_submitted_at IS NOT NULL AND DATE_TRUNC('DAY', ml.member_submitted_at) = mdml.date_day)
-- use INNER JOIN since COUNT doesn't produce NULL values.
INNER JOIN member_track_rp_stats AS rs
  ON mt.member_id = rs.member_id
  AND mt.track_id = rs.track_id
