{{ config(
    tags=["eu"],
    schema="coach"
) }}

{%- set primary_b2b_logic = "CASE WHEN t.deployment_group in ('B2B / Gov Paid Contract','BetterUp') AND ca.role = 'primary' and be.is_professional_coaching_cloud = 1" %}


WITH appointments AS (
    SELECT * FROM {{ ref('stg_app__appointments') }}
),
billable_event_session_details as (
    select
        associated_record_id,
        max(case
             when coaching_cloud = 'professional'
             then 1
             else 0 end) as is_professional_coaching_cloud
    from {{ ref('stg_app__billable_events') }}
    group by 1
),
coach_assignments AS (
    SELECT * FROM {{ ref('stg_app__coach_assignments') }}
),
track_assignments AS (
    SELECT * FROM {{ ref('stg_app__track_assignments') }}
),
product_subscription_assignments AS (
    SELECT * FROM {{ ref('int_app__product_subscription_assignments') }}
),
tracks AS (
    SELECT * FROM {{ ref('dim_tracks') }}
    WHERE track_id NOT IN (6857, 7167, 7166, 7231) -- Ipsen track IDs (see DATA-2921)
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
cso_comp_escalations AS (
    SELECT * FROM {{ ref('stg_gsheets_ops_analytics__cso_comp_escalations') }}
),

/*
window calculations for
    date of first primary b2b appointment in coach assignment
    date of most recent primary b2b appointment in coach assignment
    index of primary b2b appointment in coach assignment
    days since first primary b2b appointment in coach assignment
*/

sessions AS (

  SELECT
    ca.coach_id,
    ca.coach_assignment_id,
    app.member_id,
    app.appointment_id,
    app.complete_at AS session_complete_at,
    t.deployment_group,
    t.deployment_type,
    t.minutes_limit,
    ca.role AS coaching_assignment_role,
    be.is_professional_coaching_cloud,
    ta.ended_at AS track_assignment_ended_at,
    psa.ended_at AS product_subscription_assignment_ended_at,

    MAX({{primary_b2b_logic}} THEN coalesce(ta.ended_at,psa.ended_at,current_date) END)
        OVER (PARTITION BY ca.coach_assignment_id)
        AS primary_b2b_member_access_ended_date,

    MIN({{primary_b2b_logic}} THEN app.complete_at END)
        OVER (PARTITION BY ca.coach_assignment_id)
        AS first_completed_primary_b2b_coach_assignment_session_date,

    MAX({{primary_b2b_logic}} THEN app.complete_at END)
        OVER (PARTITION BY ca.coach_assignment_id)
        AS most_recent_completed_primary_b2b_coach_assignment_session_date


  FROM appointments AS app
  LEFT JOIN billable_event_session_details as be on be.associated_record_id = app.appointment_id
  LEFT JOIN coach_assignments AS ca ON ca.coach_assignment_id = app.coach_assignment_id
  LEFT JOIN track_assignments AS ta ON ta.track_assignment_id = app.track_assignment_id
  LEFT JOIN product_subscription_assignments AS psa ON psa.product_subscription_assignment_id = app.product_subscription_assignment_id
  LEFT JOIN tracks AS t ON t.track_id = ta.track_id
  WHERE app.is_completed -- ignore non-completed sessions
        AND t.deployment_group in ('B2B / Gov Paid Contract','BetterUp')
        AND ca.role = 'primary'
        AND be.is_professional_coaching_cloud = 1
        AND ca.coach_assignment_id NOT IN (SELECT cso.coach_assignment_id FROM cso_comp_escalations cso)

),
events_seasoned AS (
    SELECT
        dim_seasons.season_id,
        dim_seasons.season_name,
        dim_seasons.season_start_date,
        dim_seasons.season_end_date,
        sessions.coach_id,
        sessions.coach_assignment_id,
        sessions.member_id,
        case when coaching_assignment_role = 'primary'
                and deployment_group in ('B2B / Gov Paid Contract','BetterUp')
                and is_professional_coaching_cloud = 1
                and (minutes_limit > 900 or minutes_limit is null)
            then 1 else null end as primary_b2b_session_sequence_flag,
          case when
                primary_b2b_session_sequence_flag = 1
            then
            row_number() over(
              partition by
              primary_b2b_session_sequence_flag, coach_assignment_id,season_name
              order by session_complete_at asc) end
              as coach_assignment_session_sequence,
        sessions.appointment_id,
        sessions.session_complete_at,
        primary_b2b_member_access_ended_date,
        first_completed_primary_b2b_coach_assignment_session_date,
        most_recent_completed_primary_b2b_coach_assignment_session_date,
        datediff(DAY,first_completed_primary_b2b_coach_assignment_session_date, session_complete_at) AS days_since_first_completed_primary_b2b_coach_assignment_session

    FROM sessions
    LEFT JOIN dim_seasons
    ON DATEADD('day', 90, first_completed_primary_b2b_coach_assignment_session_date) >= season_start_date
    -- fan out on seasons

),
sessions_aggregated AS (
SELECT
    coaches.coach_id,
    coaches.coach_profile_uuid,
    u.user_uuid,
    coaches.full_name,
    seasons.season_name,
    COUNT(DISTINCT case
      when   dateadd('day',90, session_sequences.first_completed_primary_b2b_coach_assignment_session_date)  >=  seasons.season_start_date
        and  dateadd('day',90, session_sequences.first_completed_primary_b2b_coach_assignment_session_date)  <  seasons.season_end_date
        and  dateadd('day',90, session_sequences.first_completed_primary_b2b_coach_assignment_session_date)  <= ( session_sequences.primary_b2b_member_access_ended_date )
        and  session_sequences.coach_assignment_session_sequence   = 1 -- only first sessions
        and  dateadd('day',90, session_sequences.first_completed_primary_b2b_coach_assignment_session_date)  <= current_date
      then  session_sequences.appointment_id  end) AS primary_b2b_first_time_sessions,

    COUNT(DISTINCT case
      when   dateadd('day',90, session_sequences.first_completed_primary_b2b_coach_assignment_session_date)  >=  seasons.season_start_date
        and  dateadd('day',90, session_sequences.first_completed_primary_b2b_coach_assignment_session_date)  <  seasons.season_end_date
        and  dateadd('day',90, session_sequences.first_completed_primary_b2b_coach_assignment_session_date)  <= ( session_sequences.primary_b2b_member_access_ended_date )
        and  session_sequences.coach_assignment_session_sequence   = 5 -- only 5 consecutive sessions
        and  dateadd('day',90, session_sequences.first_completed_primary_b2b_coach_assignment_session_date) <= current_date
        and  session_sequences.session_complete_at  <= current_date
        and  session_sequences.days_since_first_completed_primary_b2b_coach_assignment_session < 90
      then  session_sequences.appointment_id  end) AS primary_b2b_5_in_90_sessions

FROM dim_coaches AS coaches
LEFT JOIN users AS u ON u.user_id = coaches.coach_id
JOIN dim_seasons AS seasons ON 1=1
LEFT JOIN events_seasoned AS session_sequences
    ON session_sequences.season_id = seasons.season_id
    AND session_sequences.coach_id = coaches.coach_id
GROUP BY 1, 2, 3, 4, 5
)

SELECT
    {{ dbt_utils.surrogate_key(['user_uuid','season_name']) }} AS user_season_id
    , sessions_aggregated.*
FROM sessions_aggregated
