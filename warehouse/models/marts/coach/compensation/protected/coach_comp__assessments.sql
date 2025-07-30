{{ config(
    tags=["eu"],
    schema="coach"
) }}

WITH billable_event_session_details as (

    select
        associated_record_id,
        max(case
             when coaching_cloud = 'professional'
             then 1
             else 0 end) as is_professional_coaching_cloud
    from {{ ref('stg_app__billable_events') }}
    group by 1
),
appointments AS (
    SELECT * FROM {{ ref('stg_app__appointments') }}
),
coach_assignments AS (
    SELECT * FROM {{ ref('stg_app__coach_assignments') }}
),
track_assignments AS (
    SELECT * FROM {{ ref('stg_app__track_assignments') }}
),
tracks AS (
    SELECT * FROM {{ ref('dim_tracks') }}
    WHERE track_id NOT IN (6857, 7167, 7166, 7231) -- Ipsen track IDs (see DATA-2921)
),
assessments AS (
    SELECT * FROM {{ ref('stg_app__assessments') }}
),
dim_seasons AS (
    SELECT * FROM {{ ref('dim_coach_compensation_seasons') }}
),
dim_coaches AS (
    SELECT * FROM {{ ref('dim_coaches') }}
),
users AS (
    SELECT * FROM {{ ref('int_app__users') }}
),
completed_assessments AS (

    SELECT
        appointments.coach_id,
        appointments.member_id,
        assessments.assessment_id,
        assessments.submitted_at,
        coach_assignments.specialist_vertical_uuid,
        coach_assignments.specialist_vertical_id,
        coach_assignments.role AS coaching_assignment_role,
        tracks.deployment_group,
        tracks.deployment_type,
        is_professional_coaching_cloud

    FROM appointments
    LEFT JOIN assessments
        ON appointments.post_session_member_assessment_id = assessments.assessment_id
    left join billable_event_session_details
        on billable_event_session_details.associated_record_id = appointments.appointment_id
    LEFT JOIN coach_assignments
        ON coach_assignments.coach_assignment_id = appointments.coach_assignment_id
    LEFT JOIN track_assignments
        ON track_assignments.track_assignment_id = appointments.track_assignment_id
    LEFT JOIN tracks
        ON tracks.track_id = track_assignments.track_id
    WHERE
        appointments.is_completed --ignore non completed sessions
        AND appointments.post_session_member_assessment_id IS NOT NULL

),
post_session_assessment_scores as (

  select
    assessments.assessment_id,
    response.value as assessment_response,
    case
      when assessment_response = 'Life Changing' then 5
      when assessment_response = 'Amazing' then 4
      when assessment_response = 'Good' then 3
      when assessment_response = 'Okay' then 2
      when assessment_response = 'Not Great' then 1
    end as assessment_response_score
  from assessments
  inner join lateral flatten
    (input => assessments.responses) as response
  where assessments.type = 'Assessments::PostSessionMemberAssessment'
  and response.path  = 'session_overall_emotional'
),
assessments_details AS (

    select
        completed_assessments.*
        , post_session_assessment_scores.assessment_response_score
    from completed_assessments
    left join post_session_assessment_scores
        on post_session_assessment_scores.assessment_id = completed_assessments.assessment_id
),

fact_coach_compensation_events_seasoned AS (
    SELECT
    dim_seasons.season_id,
    dim_seasons.season_start_date,
    dim_seasons.season_end_date,
    assessments_details.*
        FROM assessments_details
        LEFT JOIN dim_seasons
            on assessments_details.submitted_at >= dim_seasons.season_start_date
            and assessments_details.submitted_at < dim_seasons.season_end_date
),
    final AS (
    SELECT
        dim_coaches.coach_id,
        dim_coaches.coach_profile_uuid,
        u.user_uuid,
        dim_coaches.full_name,
        dim_coach_compensation_seasons.season_name,

    --    ASSESSMENTS
        COUNT(DISTINCT CASE
          WHEN assessment_response_score >= 1 THEN fact_coach_compensation_events_seasoned.assessment_id
        END) AS lifetime_assessments,

        COUNT(DISTINCT CASE
          WHEN assessment_response_score >= 1
              AND ((coaching_assignment_role = 'primary'
              AND deployment_group IN ('B2B / Gov Paid Contract','BetterUp')
              AND fact_coach_compensation_events_seasoned.is_professional_coaching_cloud = 1)
              OR specialist_vertical_uuid = 'c8db04ad-3d63-4b23-bef3-fa32258a4824')
          THEN fact_coach_compensation_events_seasoned.assessment_id
        END) AS primary_b2b_assessments,

    --    LC/A ASSESSMENTS
       COUNT(DISTINCT CASE
          WHEN assessment_response_score >= 4 THEN fact_coach_compensation_events_seasoned.assessment_id
        END) AS lifetime_lca_assessments,

       COUNT(DISTINCT CASE
          WHEN assessment_response_score >= 4
              AND ((coaching_assignment_role = 'primary'
              AND deployment_group IN ('B2B / Gov Paid Contract','BetterUp')
              AND fact_coach_compensation_events_seasoned.is_professional_coaching_cloud = 1)
              OR specialist_vertical_uuid = 'c8db04ad-3d63-4b23-bef3-fa32258a4824')
          THEN fact_coach_compensation_events_seasoned.assessment_id
        END) AS primary_b2b_lca_assessments

    FROM dim_coaches
    LEFT JOIN users AS u ON u.user_id = dim_coaches.coach_id
    LEFT JOIN dim_seasons AS dim_coach_compensation_seasons ON 1=1
    LEFT JOIN fact_coach_compensation_events_seasoned ON fact_coach_compensation_events_seasoned.season_id = dim_coach_compensation_seasons.season_id
          AND fact_coach_compensation_events_seasoned.coach_id = dim_coaches.coach_id
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    {{ dbt_utils.surrogate_key(['user_uuid','season_name']) }} AS user_season_id
    , final.*
FROM final
