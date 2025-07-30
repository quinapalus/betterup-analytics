{{ config(
    tags=['lead_metrics','eu'],
) }}

WITH reporting_group_assignments AS (
  SELECT * FROM {{ref('dim_reporting_group_assignments')}}
  ),

primary_coach_selection AS (
  SELECT
    rga.member_id,
    rga.reporting_group_id,
    MIN(attributes:"coach_assignment_date"::timestamp) AS primary_coach_selected_at
  FROM reporting_group_assignments AS rga
  INNER JOIN {{ref('dbt_events__scheduled_appointment')}}  AS sa
      ON rga.member_id = sa.member_id AND
       sa.event_at >= rga.starts_at AND
       (rga.ended_at IS NULL OR sa.event_at < rga.ended_at) AND
      sa.event_object = 'primary_appointment'
GROUP BY rga.member_id, rga.reporting_group_id
  ),

lead_onboarding_metrics AS (
  SELECT * FROM {{ref('dbt_member_reporting_group_lead__onboarding_metrics')}}
  ),

submitted_rp_assessment_metrics AS (
  SELECT * FROM {{ref('dbt_member_reporting_group__submitted_reflection_point_assessment_metrics')}}
  ),

lead_completed_session_metrics AS (
  SELECT * FROM {{ref('dbt_member_reporting_group_lead__completed_session_metrics')}}
  ),

lead_upcoming_session_metrics AS (
  SELECT * FROM {{ref('dbt_member_reporting_group_lead__upcoming_session_metrics')}}
  ),

lead_current_reflection_point_status AS (
  SELECT * FROM {{ref('dbt_member_reporting_group_lead__current_reflection_point_status')}}
  ),

completed_activity_metrics AS (
  SELECT * FROM {{ref('dbt_member_reporting_group__completed_activity_metrics')}}
  ),

user_engaged_metrics AS (
  SELECT * FROM {{ref('dbt_member_reporting_group__user_engaged_metrics')}}
  ),

wp360_metrics AS (
     SELECT * FROM {{ref('dbt_member_reporting_group__wp360_metrics')}}
  ),

member_reporting_groups AS (
  SELECT
    member_id,
    reporting_group_id,
    RANK()OVER(PARTITION BY member_id ORDER BY max(COALESCE(ended_at, '9999-12-31')) DESC) AS rank,
    IFF(rank = 1, TRUE, FALSE) AS most_recent_reporting_group,
    BOOLOR_AGG(member_is_open) AS member_is_open
  FROM reporting_group_assignments
  GROUP BY member_id, reporting_group_id
  ),

onboarded_assessment_events AS (
  SELECT member_id, min(event_at) as completed_onboarding_assessment_at
  FROM {{ref('fact_member_events')}}
  WHERE event_name = 'submitted assessment' AND
        attributes:"assessment_type"::VARCHAR IN (
      'Assessments::OnboardingAssessment',
      'Assessments::PrimaryCoachingModalitySetupAssessment',
      'Assessments::WholePersonAssessment'
      )
  GROUP BY 1
  )

SELECT
  {{ dbt_utils.surrogate_key(['mrg.member_id', 'mrg.reporting_group_id']) }} AS primary_key,
  mrg.member_id,
  mrg.reporting_group_id,
  mrg.most_recent_reporting_group,
  mrg.member_is_open AS current_reporting_group,
  CASE
    WHEN NOT mrg.member_is_open OR lo.lead_access_ended_at < CURRENT_TIMESTAMP THEN 'ended'
    WHEN lcs.first_session_at IS NOT NULL THEN 'started coaching'
    WHEN oae.completed_onboarding_assessment_at IS NOT NULL THEN 'completed_onboarding'
    WHEN lo.activated_at IS NOT NULL THEN 'activated'
    ELSE 'invited'
  END AS member_status,
  lo.first_psa_starts_at,
  lo.program_invite_at,
  lo.program_ended_at,
  lo.lead_access_ended_at,
  lo.activated_at,
  --lo.completed_onboarding_at,
  oae.completed_onboarding_assessment_at as completed_onboarding_at,
  pcs.primary_coach_selected_at,
  lo.product_name AS active_product_subscription,
  rp.submitted_last_reflection_point_at,
  rp.count_completed_reflection_points,
  lcrp.current_reflection_point_status,
  lus.next_session_at,
  lus.next_primary_session_at,
  lcs.first_session_at,
  lcs.first_primary_session_at,
  lcs.last_session_at,
  lcs.last_primary_session_at,
  lcs.completed_session_count,
  lcs.completed_session_hours,
  lcs.average_session_length_hours,
  lcs.completed_primary_session_count,
  lcs.completed_extended_network_session_count,
  lcs.completed_on_demand_session_count,
  ca.completed_resource_count,
  ue.last_engagement_at,
  wp_360_status
FROM member_reporting_groups AS mrg
INNER JOIN lead_onboarding_metrics AS lo
  ON mrg.member_id = lo.member_id AND
     mrg.reporting_group_id = lo.reporting_group_id
LEFT OUTER JOIN onboarded_assessment_events AS oae
  ON mrg.member_id = oae.member_id
LEFT OUTER JOIN submitted_rp_assessment_metrics as rp
  ON mrg.member_id = rp.member_id AND
     mrg.reporting_group_id = rp.reporting_group_id
LEFT OUTER JOIN lead_completed_session_metrics AS lcs
  ON mrg.member_id = lcs.member_id AND
     mrg.reporting_group_id = lcs.reporting_group_id
LEFT OUTER JOIN lead_upcoming_session_metrics AS lus
  ON mrg.member_id = lus.member_id AND
     mrg.reporting_group_id = lus.reporting_group_id
LEFT OUTER JOIN lead_current_reflection_point_status AS lcrp
 ON mrg.member_id = lcrp.member_id AND
    mrg.reporting_group_id = lcrp.reporting_group_id
LEFT OUTER JOIN completed_activity_metrics AS ca
  ON mrg.member_id = ca.member_id AND
     mrg.reporting_group_id = ca.reporting_group_id
LEFT OUTER JOIN user_engaged_metrics AS ue
  ON mrg.member_id = ue.member_id AND
     mrg.reporting_group_id = ue.reporting_group_id
LEFT OUTER JOIN wp360_metrics AS wp360_metrics
  ON mrg.member_id = wp360_metrics.member_id AND
     mrg.reporting_group_id = wp360_metrics.reporting_group_id
LEFT OUTER JOIN primary_coach_selection AS pcs
  ON mrg.member_id = pcs.member_id AND
     mrg.reporting_group_id = pcs.reporting_group_id