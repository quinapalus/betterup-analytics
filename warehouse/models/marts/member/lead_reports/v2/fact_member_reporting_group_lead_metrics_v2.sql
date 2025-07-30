{{ config(
    tags=['lead_metrics','eu'],
) }}

WITH reporting_group_assignments AS (

  SELECT * FROM {{ref('dim_reporting_group_assignments')}}
  WHERE ASSOCIATED_RECORD_TYPE = 'Track'

),

product_subscription_assignment_extension_logs AS (
  SELECT psa.member_id
  FROM {{ref('stg_app__product_subscription_assignment_extension_logs')}} AS psael
  INNER JOIN {{ ref('int_app__product_subscription_assignments') }} AS psa
     ON psael.product_subscription_assignment_id = psa.product_subscription_assignment_id
     AND CURRENT_TIMESTAMP::DATE - psael.created_at::DATE <= 30
  GROUP BY psa.member_id

),

reporting_group_engagement_metrics AS (

  SELECT *,
      ROW_NUMBER()OVER(PARTITION BY member_id ORDER BY ended_at DESC NULLS LAST) AS lead_access_ended_at,
      ROW_NUMBER()OVER(PARTITION BY member_id ORDER BY started_at NULLS LAST) AS first_psa_starts_at
  FROM {{ref('fact_reporting_group_engagement_metrics')}}
  WHERE reporting_group_id in (
        SELECT reporting_group_id FROM  {{ref('dim_reporting_groups')}}
        WHERE product_type IN ( 'growth_and_transformation',
                                'sales_performance',
                                'diversity_equity_inclusion_and_belonging',
                                'primary_coaching'
         )
    )

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

  SELECT *
  FROM {{ref('dbt_member_reporting_group_lead__onboarding_metrics')}}

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

manager_engagement_metrics AS (

     SELECT * FROM {{ref('dbt_member_reporting_group__manager_engagement_metrics')}}
     QUALIFY ROW_NUMBER()OVER(PARTITION BY reporting_group_id, member_id ORDER BY manager_invited_at DESC) = 1

),

contributor_engagement_metrics AS (

     SELECT * FROM {{ref('dbt_member_reporting_group__contributor_engagement_metrics')}}
     QUALIFY ROW_NUMBER()OVER(PARTITION BY reporting_group_id, member_id ORDER BY contributor_invited_at DESC) = 1

),

onboarded_assessment_events AS (

  SELECT member_id, min(event_at) as event_at FROM {{ref('fact_member_events')}}
  WHERE event_name = 'submitted assessment' AND
        attributes:"assessment_type"::VARCHAR IN (
      'Assessments::OnboardingAssessment',
      'Assessments::PrimaryCoachingModalitySetupAssessment',
      'Assessments::WholePersonAssessment'
      )
  GROUP BY 1

),

member_reporting_groups AS (

  SELECT
    member_id,
    reporting_group_id,
    MIN(starts_at) AS starts_at,
    NULLIF(MAX(COALESCE(ended_at, '9999-12-31')), '9999-12-31') AS ended_at,
    RANK()OVER(PARTITION BY member_id ORDER BY MAX(COALESCE(ended_at, '9999-12-31')) DESC) AS rank,
    IFF(rank = 1, TRUE, FALSE) AS most_recent_reporting_group,
    BOOLOR_AGG(member_is_open) AS member_is_open
  FROM reporting_group_assignments
  GROUP BY member_id, reporting_group_id

)


SELECT
  {{ dbt_utils.surrogate_key(['rgem.member_id', 'rgem.track_name', 'rgem.reporting_group_id']) }} AS primary_key,
  rgem.member_id,
  rgem.reporting_group_id,
  rgem.track_name,
  mrg.most_recent_reporting_group,
  mrg.member_is_open AS current_reporting_group,
  rgem.status AS member_status,
  psa_starts.started_at AS first_psa_starts_at,
  mrg.starts_at AS program_invite_at,
  mrg.ended_at AS program_ended_at,
  access_ends.ended_at AS lead_access_ended_at,
  lo.activated_at,
  lo.completed_onboarding_at,
  oae.event_at AS completed_onboarding_assessment_at,
  pcs.primary_coach_selected_at,
  lo.product_name AS active_product_subscription,
  rp.submitted_last_reflection_point_at,
  rp.count_completed_reflection_points,
  lcrp.current_reflection_point_status,
  rgem.next_session_at,
  lus.next_primary_session_at,
  lcs.first_session_at,
  lcs.first_primary_session_at,
  rgem.last_session_at,
  lcs.last_primary_session_at,
  rgem.total_sessions AS completed_session_count,
  lcs.completed_session_hours,
  lcs.average_session_length_hours,
  lcs.completed_primary_session_count,
  lcs.completed_extended_network_session_count,
  lcs.completed_on_demand_session_count,
  ca.completed_resource_count,
  rgem.last_engaged_at AS last_engagement_at,
  wp_360_status,
  mem.manager_id,
  mem.manager_invited_at,
  mem.manager_feedback_submitted_at,
  mem.manager_growth_assessment_submitted_at,
  psael.member_id AS is_recently_extended,
  cem.contributor_id,
  cem.contributor_invited_at,
  cem.first_feedback_submitted_at,
  cem.last_feedback_submitted_at
FROM reporting_group_engagement_metrics AS rgem
INNER JOIN reporting_group_engagement_metrics AS access_ends
  ON access_ends.member_id = rgem.member_id
      And access_ends.lead_access_ended_at = 1
INNER JOIN reporting_group_engagement_metrics AS psa_starts
  ON psa_starts.member_id = rgem.member_id AND
     psa_starts.first_psa_starts_at = 1
INNER JOIN member_reporting_groups AS mrg
  ON mrg.member_id = rgem.member_id AND
     mrg.reporting_group_id = rgem.reporting_group_id
LEFT OUTER JOIN onboarded_assessment_events AS oae
  ON mrg.member_id = oae.member_id
LEFT OUTER JOIN product_subscription_assignment_extension_logs as psael
  ON psael.member_id = mrg.member_id
LEFT OUTER JOIN lead_onboarding_metrics AS lo
  ON mrg.member_id = lo.member_id AND
     mrg.reporting_group_id = lo.reporting_group_id
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
LEFT OUTER JOIN manager_engagement_metrics AS mem
  ON mrg.member_id = mem.member_id AND
     mrg.reporting_group_id = mem.reporting_group_id
LEFT OUTER JOIN contributor_engagement_metrics AS cem
  ON mrg.member_id = cem.member_id AND
     mrg.reporting_group_id = cem.reporting_group_id
