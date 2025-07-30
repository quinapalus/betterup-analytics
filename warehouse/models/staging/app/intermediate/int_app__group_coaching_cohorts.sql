WITH group_coaching_cohorts AS (
    SELECT * FROM {{ ref('stg_app__group_coaching_cohorts') }}
),
group_coaching_config_session_duration_settings AS (
    SELECT * FROM {{ ref('stg_app__group_coaching_config_session_duration_settings') }}
),
group_coaching_sessions AS (
    SELECT * FROM {{ ref('stg_app__group_coaching_sessions') }}
)

SELECT
        DISTINCT gc_cohorts.group_coaching_cohort_id,
        gc_cohorts.group_coaching_series_id,
        gc_cohorts.coach_id,
        gc_config_session_duration_settings.duration_minutes AS session_duration_minutes,
        gc_cohorts.remaining_seats,
        gc_cohorts.seat_capacity,
        gc_cohorts.min_seat_count,
        gc_cohorts.is_live_session,
        gc_cohorts.first_session_starts_at,
        gc_cohorts.ended_at,
        gc_cohorts.canceled_at,
        gc_cohorts.created_at,
        gc_cohorts.updated_at
FROM group_coaching_cohorts AS gc_cohorts
INNER JOIN group_coaching_sessions AS gc_sessions
    ON gc_sessions.group_coaching_cohort_id = gc_cohorts.group_coaching_cohort_id
INNER JOIN group_coaching_config_session_duration_settings AS gc_config_session_duration_settings
    ON gc_config_session_duration_settings.session_duration_configurable_id = gc_sessions.group_coaching_session_id
WHERE gc_config_session_duration_settings.session_duration_configurable_type = 'GroupCoachingSession'

-- We use a DISTINCT because, right now, all the cohorts have the same session duration for each individual
-- session, so just grabbing one for each cohort will suffice, but in the future
-- (we don't know when) we may introduce cohorts with variable session durations.