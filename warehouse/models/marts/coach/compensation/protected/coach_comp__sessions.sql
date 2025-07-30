{{ config(
    tags=["eu"],
    schema="coach"
) }}

WITH billable_events AS (
    SELECT * FROM {{ ref('stg_app__billable_events') }}
),
track_assignments AS (
    SELECT * FROM {{ ref('stg_app__track_assignments') }}
),
tracks AS (
    SELECT * FROM {{ ref('dim_tracks') }}
),
appointments AS (
    SELECT * FROM {{ ref('stg_app__appointments') }}
),
coach_assignments AS (
    SELECT * FROM {{ ref('stg_app__coach_assignments') }}
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
billable_event_details AS (

    SELECT
    be.billable_event_id,
    be.coach_id,
    be.member_id,
    be.event_at,
    ca.specialist_vertical_uuid,
    t.deployment_group,
    t.deployment_type,
    be.coaching_cloud,
    ca.role AS coaching_assignment_role,
    be.event_type AS billing_event_type,
    be.associated_record_id AS billable_event_associated_record_id,
    be.units AS billable_event_hours,
    be.amount_due_usd,
    t.minutes_limit

    FROM billable_events be
    LEFT JOIN appointments AS a ON a.appointment_id = be.ASSOCIATED_RECORD_ID
    LEFT JOIN coach_assignments AS ca ON ca.coach_assignment_id = a.COACH_ASSIGNMENT_ID
    LEFT JOIN track_assignments AS ta ON ta.track_assignment_id = a.TRACK_ASSIGNMENT_ID
    LEFT JOIN tracks AS t ON t.track_id = ta.track_id
),
fact_coach_compensation_events_seasoned AS (
    SELECT
    dim_seasons.season_id,
    dim_seasons.season_start_date,
    dim_seasons.season_end_date,
    billable_event_details.*
        FROM billable_event_details
        LEFT JOIN dim_seasons
            ON billable_event_details.event_at <= dim_seasons.season_end_date
),
final AS (
    SELECT
        dim_coaches.coach_id,
        dim_coaches.coach_profile_uuid,
        u.user_uuid,
        dim_coaches.full_name,
        dim_coach_compensation_seasons.season_name,

        COUNT(DISTINCT CASE
          WHEN fact_coach_compensation_events_seasoned.billing_event_type IN ('completed_sessions','completed_group_sessions')
          THEN fact_coach_compensation_events_seasoned.billable_event_id
        END) AS lifetime_sessions,

        COUNT(DISTINCT CASE
          WHEN fact_coach_compensation_events_seasoned.billing_event_type IN ('completed_sessions')
              AND ((coaching_assignment_role = 'primary'
              AND deployment_group IN ('B2B / Gov Paid Contract','BetterUp')
              AND fact_coach_compensation_events_seasoned.coaching_cloud = 'professional')
              OR specialist_vertical_uuid = 'c8db04ad-3d63-4b23-bef3-fa32258a4824')
          THEN fact_coach_compensation_events_seasoned.billable_event_id
        END) AS primary_b2b_lifetime_sessions

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
