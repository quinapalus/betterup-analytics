WITH group_coaching_registrations AS (

  SELECT * FROM {{ ref('stg_app__group_coaching_registrations') }}
  WHERE canceled_at IS NULL
),

group_coaching_cohorts AS (

  SELECT * FROM {{ ref('stg_app__group_coaching_cohorts') }}

),

group_coaching_series AS (

  SELECT * FROM {{ ref('stg_app__group_coaching_series') }}

),

group_coaching_curriculums AS (

  SELECT * FROM {{ ref('stg_app__group_coaching_curriculums') }}

)


SELECT
  -- Surrogate Key of Member ID + Associated Record ID. Functions as Primary Key. Matching name format of downstream PKs.
  {{ dbt_utils.surrogate_key(['r.user_id', 'r.group_coaching_registration_id']) }} AS primary_key,
  r.user_id AS member_id,
  r.group_coaching_registration_id AS associated_record_id,
  'GroupCoachingRegistration' AS associated_record_type,
  r.created_at AS feature_collected_at,
  'group_coaching_topic' AS feature_key,
  OBJECT_CONSTRUCT('label', cu.title) AS classification,
  OBJECT_CONSTRUCT('intervention_type', cu.intervention_type) AS feature_attributes,
  'coaching_topic' AS feature_type
FROM group_coaching_registrations AS r
INNER JOIN group_coaching_cohorts AS co
  ON r.group_coaching_cohort_id = co.group_coaching_cohort_id
INNER JOIN group_coaching_series AS s
  ON co.group_coaching_series_id = s.group_coaching_series_id
INNER JOIN group_coaching_curriculums AS cu
  ON s.group_coaching_curriculum_id = cu.group_coaching_curriculum_id