WITH group_coaching_appointments AS (

  SELECT * FROM {{ ref('stg_app__group_coaching_appointments') }}

),

group_coaching_sessions AS (

  SELECT * FROM {{ ref('stg_app__group_coaching_sessions') }}

),

group_coaching_cohorts AS (

  SELECT * FROM {{ ref('int_app__group_coaching_cohorts') }}

),

group_coaching_series AS (

  SELECT * FROM {{ ref('stg_app__group_coaching_series') }}

),

group_coaching_curriculums AS (

  SELECT * FROM {{ ref('stg_app__group_coaching_curriculums') }}

)


SELECT
  --surrogate primary key
  {{ dbt_utils.surrogate_key(['a.member_id', 'group_coaching_appointment_id']) }} AS member_associated_record_id,
  a.member_id,
  a.attempted_to_join_at AS event_at,
  'attended' AS event_action,
  cu.intervention_type || '_appointment' AS event_object,
  event_action || ' ' || event_object AS event_action_and_object,
  'GroupCoachingAppointment' AS associated_record_type,
  a.group_coaching_appointment_id AS associated_record_id,
  OBJECT_CONSTRUCT('group_coaching_cohort_id', co.group_coaching_cohort_id,
                   'group_coaching_series_id', sr.group_coaching_series_id,
                   'session_duration_minutes', co.session_duration_minutes
                  ) AS attributes
FROM group_coaching_appointments AS a
INNER JOIN group_coaching_sessions AS s
  ON a.group_coaching_session_id = s.group_coaching_session_id
INNER JOIN group_coaching_cohorts AS co
  ON s.group_coaching_cohort_id = co.group_coaching_cohort_id
INNER JOIN group_coaching_series AS sr
  ON co.group_coaching_series_id = sr.group_coaching_series_id
INNER JOIN group_coaching_curriculums AS cu
  ON sr.group_coaching_curriculum_id = cu.group_coaching_curriculum_id
WHERE a.attempted_to_join_at IS NOT NULL
