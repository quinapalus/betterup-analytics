{{ config(
  enabled = false
) }}

with members AS (

        SELECT *
        FROM {{ref('members')}}
    ),

     started_assessment AS (

         SELECT *
         FROM {{ref('dbt_member_reporting_group__started_assessments_grouped')}}
     ),

     submitted_assessment AS (

         SELECT *
         FROM {{ref('dbt_member_reporting_group__submitted_assessments_grouped')}}
     ),

     completed_session AS (

         SELECT *
         FROM {{ref('dbt_member_reporting_group__completed_sessions_grouped')}}
     ),

     completed_activity AS (

         SELECT *
         FROM {{ref('dbt_member_reporting_group__completed_activites_grouped')}}
     ),

     member_engagement_events AS (

         SELECT *
         FROM {{ref('dbt_member_reporting_group__engagement_events_grouped')}}
     ),

     reporting_group_assignments_grouped AS (

         SELECT * FROM {{ref('dbt_member_reporting_groups')}}
     ),

     next_appointment_at AS (
         SELECT member_id,
                starts_at,
                track_assignment_id
         FROM {{ ref('stg_app__appointments') }}
         WHERE starts_at > current_timestamp
         AND canceled_at IS NULL
         AND complete_at IS NULL
         AND NOT missed
         QUALIFY ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY starts_at) = 1
),
     product_subscription AS (

         SELECT
            member_id,
            MAX(ends_at)                             AS primary_coaching_ends_at,
            MIN(starts_at)                           AS invited_at
        FROM {{ref('dim_reporting_group_assignments')}}
        WHERE ended_at IS NULL
        AND associated_record_type = 'ProductSubscription'
        GROUP BY 1

     )


SELECT
    {{ dbt_utils.surrogate_key(['m.member_id', 'rga.reporting_group_id']) }} AS primary_key,
    m.member_id,
    rga.reporting_group_id,
    m.activated_at,
    rga.starts_at AS invited_at,
    ps.primary_coaching_ends_at AS product_access_end_at,
    cs.first_session_at,
    naa.starts_at AS next_appointment_at,
    sta.onboarding_assessment_created_at,
    sua.submitted_at AS onboarding_assessment_submitted_at,
    COALESCE(cs.completed_sessions, 0) AS completed_sessions,
    COALESCE( cs.completed_on_demand_sessions, 0) AS completed_on_demand_sessions,
    COALESCE(cs.completed_extended_network_sessions, 0) AS completed_extended_network_sessions,
    COALESCE(cs.completed_session_hours, 0) AS completed_session_hours,
    COALESCE(cs.average_session_length_hours, 0) AS average_session_length_hours,
    COALESCE(ca.completed_resource_minutes, 0) AS completed_resource_minutes,
    COALESCE(ca.completed_resources, 0) AS completed_resources,
    CASE WHEN rga.member_is_open THEN m.last_engaged_at END AS last_engaged_at,
    cs.last_session_at,
    rp.submitted_at AS reflection_point_completed_at
FROM members AS m
INNER JOIN reporting_group_assignments_grouped AS rga
    ON m.member_id = rga.member_id
LEFT JOIN next_appointment_at AS naa
    ON m.member_id = naa.member_id
    AND rga.associated_assignment_id = naa.track_assignment_id
LEFT JOIN started_assessment AS sta
    ON rga.member_id = sta.member_id
LEFT JOIN submitted_assessment AS sua
    ON rga.member_id = sua.member_id
    AND sua.assessment_type = 'Assessments::OnboardingAssessment'
LEFT JOIN submitted_assessment AS rp
    ON rga.member_id = rp.member_id
    AND rp.assessment_type = 'Assessments::WholePersonProgramCheckinAssessment'
    AND rga.reporting_group_id = rp.reporting_group_id
    AND  rga.associated_assignment_id = rp.associated_assignment_id
LEFT JOIN completed_session AS cs
    ON rga.member_id = cs.member_id
    AND rga.reporting_group_id = cs.reporting_group_id
    AND  rga.associated_assignment_id = cs.associated_assignment_id
LEFT JOIN completed_activity AS ca
    ON rga.member_id = ca.member_id
    AND rga.reporting_group_id = ca.reporting_group_id
    AND  rga.associated_assignment_id = ca.associated_assignment_id
LEFT JOIN product_subscription AS ps
    ON rga.member_id = ps.member_id
