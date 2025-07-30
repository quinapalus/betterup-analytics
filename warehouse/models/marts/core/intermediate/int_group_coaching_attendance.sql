{{
  config(
    tags=['classification.c3_confidential'],
    materialized='table'
  )
}}

WITH group_coaching_sessions AS (
    SELECT *, to_char(starts_at, 'YYYYMMDD') AS session_date_key FROM {{ref('stg_app__group_coaching_sessions')}}
),

group_coaching_cohorts AS (
    SELECT * FROM {{ref('stg_app__group_coaching_cohorts')}}
),


group_coaching_registrations AS (
    SELECT * FROM {{ref('stg_app__group_coaching_registrations')}}
),


group_coaching_appointments AS (
    SELECT * FROM {{ref('int_app__group_coaching_appointments')}}
),

group_coaching_series AS (
    SELECT * FROM {{ref('stg_app__group_coaching_series')}}
),

group_coaching_curriculums AS (
    SELECT * FROM {{ref('stg_app__group_coaching_curriculums')}}
),

group_coaching_series_track_assignments AS (
    SELECT * FROM {{ref('stg_app__group_coaching_series_track_assignments')}}
),

track_assignments AS (
    SELECT * FROM {{ref('stg_app__track_assignments')}}
),

tracks AS (
    SELECT * FROM {{ref('dim_tracks')}}
),

completed_session_attendance AS (
          SELECT 'completed' AS status, gcs.group_coaching_cohort_id,
                 gc.intervention_type,
                 gs.group_coaching_series_id,
                 gcs.group_coaching_session_id,
                 gcs.ends_at AS session_date,
                 gcs.session_number AS session_order,
                 gca.member_id,
                 CASE WHEN gcr.user_id IS NOT NULL THEN 'Yes' ELSE 'No' END AS member_registered,
                 CASE WHEN gca.attempted_to_join_at IS NOT NULL THEN 'Yes' ELSE 'No' END AS member_attempted_to_join,
                 gcc.seat_capacity AS max_registrants,
                 gcc.min_seat_count AS min_registrants
              FROM group_coaching_sessions AS gcs
              INNER JOIN group_coaching_cohorts AS gcc
              ON gcc.group_coaching_cohort_id = gcs.group_coaching_cohort_id
              INNER JOIN group_coaching_registrations AS gcr
              ON gcr.group_coaching_cohort_id = gcc.group_coaching_cohort_id
              INNER JOIN group_coaching_appointments AS gca
              ON gca.group_coaching_session_id = gcs.group_coaching_session_id AND gcr.user_id = gca.member_id
              INNER JOIN group_coaching_series AS gs
              ON gs.group_coaching_series_id = gcc.group_coaching_series_id
              INNER JOIN group_coaching_curriculums AS gc
              ON gs.group_coaching_curriculum_id = gc.group_coaching_curriculum_id
              WHERE gcc.canceled_at IS NULL -- picking up cohorts which are not canceled
              AND gcr.canceled_at IS NULL -- picking up registrants who have not canceled
              AND gcr.created_at < gcs.starts_at -- picking up registrants created before a specific session starts
),

canceled_session_attendance AS (
          SELECT 'canceled' AS status, gcs.group_coaching_cohort_id,
                 gc.intervention_type,
                 gs.group_coaching_series_id,
                 gcs.group_coaching_session_id,
                 gcs.ends_at AS session_date,
                 gcs.session_number AS session_order,
                 gca.member_id,
                 CASE WHEN gcr.user_id IS NOT NULL THEN 'Yes' ELSE 'No' END AS member_registered,
                 CASE WHEN gca.attempted_to_join_at IS NOT NULL THEN 'Yes' ELSE 'No' END AS member_attempted_to_join,
                 gcc.seat_capacity AS max_registrants,
                 gcc.min_seat_count AS min_registrants
            FROM group_coaching_cohorts AS gcc
            INNER JOIN group_coaching_sessions AS gcs
            ON gcs.group_coaching_cohort_id = gcc.group_coaching_cohort_id
            INNER JOIN group_coaching_registrations AS gcr
            ON gcr.group_coaching_cohort_id = gcc.group_coaching_cohort_id
            INNER JOIN group_coaching_appointments AS gca
            ON gca.group_coaching_session_id = gcs.group_coaching_session_id AND gcr.user_id = gca.member_id
            INNER JOIN group_coaching_series AS gs
            ON gs.group_coaching_series_id = gcc.group_coaching_series_id
            INNER JOIN group_coaching_curriculums AS gc
            ON gs.group_coaching_curriculum_id = gc.group_coaching_curriculum_id
            WHERE gcc.canceled_at IS NOT NULL -- only canceled cohorts
)

SELECT 
    -- Surrogate Key of Member ID, Cohort ID, Series ID, and Session ID
    {{ dbt_utils.surrogate_key(['member_id', 'group_coaching_cohort_id', 'group_coaching_series_id', 'group_coaching_session_id']) }} AS primary_key,
    * 
FROM completed_session_attendance

UNION

SELECT
    -- Surrogate Key of Member ID, Cohort ID, Series ID, and Session ID
    {{ dbt_utils.surrogate_key(['member_id', 'group_coaching_cohort_id', 'group_coaching_series_id', 'group_coaching_session_id']) }} AS primary_key,
    * 
FROM canceled_session_attendance