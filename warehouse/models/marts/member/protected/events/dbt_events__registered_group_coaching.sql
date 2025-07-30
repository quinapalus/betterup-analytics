WITH group_coaching_registrations AS (

  SELECT * FROM {{ ref('stg_app__group_coaching_registrations') }}

),

group_coaching_cohorts AS (

  SELECT * FROM {{ ref('stg_app__group_coaching_cohorts') }}

),

group_coaching_series AS (

  SELECT * FROM {{ ref('stg_app__group_coaching_series') }}

),

group_coaching_sessions AS (

  SELECT * FROM {{ ref('stg_app__group_coaching_sessions') }}

),

group_coaching_curriculums AS (

  SELECT * FROM {{ ref('stg_app__group_coaching_curriculums') }}

),

group_coaching_series_attributes AS (

  SELECT
    co.group_coaching_cohort_id,
    co.first_session_starts_at AS group_coaching_cohort_first_session_starts_at,
    COUNT(DISTINCT s.group_coaching_session_id) AS group_coaching_cohort_session_count,
    MAX(s.starts_at) AS group_coaching_cohort_last_session_starts_at
  FROM group_coaching_cohorts AS co
  INNER JOIN group_coaching_sessions AS s
    ON co.group_coaching_cohort_id = s.group_coaching_cohort_id
  GROUP BY co.group_coaching_cohort_id, co.first_session_starts_at

),

final AS (

    SELECT
      r.user_id AS member_id,
      r.created_at AS event_at,
      'registered' AS event_action,
      cu.intervention_type AS event_object,
      event_action || ' ' || event_object AS event_action_and_object,
      'GroupCoachingRegistration' AS associated_record_type,
      r.group_coaching_registration_id AS associated_record_id,
      OBJECT_CONSTRUCT('group_coaching_cohort_id', co.group_coaching_cohort_id,
                       'group_coaching_series_id', s.group_coaching_series_id,
                       'group_coaching_cohort_session_count', sa.group_coaching_cohort_session_count,
                       'group_coaching_cohort_first_session_starts_at', sa.group_coaching_cohort_first_session_starts_at,
                       'group_coaching_cohort_last_session_starts_at', sa.group_coaching_cohort_last_session_starts_at,
                       'group_coaching_curriculum_title', cu.title
                      ) AS attributes
    FROM group_coaching_registrations AS r
    INNER JOIN group_coaching_cohorts AS co
      ON r.group_coaching_cohort_id = co.group_coaching_cohort_id
    INNER JOIN group_coaching_series AS s
      ON co.group_coaching_series_id = s.group_coaching_series_id
    INNER JOIN group_coaching_series_attributes AS sa
      ON co.group_coaching_cohort_id = sa.group_coaching_cohort_id
    INNER JOIN group_coaching_curriculums AS cu
      ON s.group_coaching_curriculum_id = cu.group_coaching_curriculum_id
    WHERE r.canceled_at IS NULL
)

SELECT
    final.*
FROM final