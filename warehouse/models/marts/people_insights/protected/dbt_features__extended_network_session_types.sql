WITH appointments AS (

  SELECT * FROM {{ ref('stg_app__appointments') }}

),

specialist_verticals AS (

  SELECT * FROM {{ ref('stg_curriculum__specialist_verticals') }}

),

coach_assignments AS (

  SELECT * FROM {{ ref('stg_app__coach_assignments') }}

)


SELECT DISTINCT
  a.member_id AS member_id,
  a.appointment_id AS associated_record_id,
  'Appointment' AS associated_record_type,
  a.starts_at AS feature_collected_at,
  'extended_network_coaching_session' AS feature_key,
  OBJECT_CONSTRUCT('key', sv.key, 'label', sv.name) AS classification,
  OBJECT_CONSTRUCT('coach_type', ca.role) AS feature_attributes,
  'engagement' AS feature_type
FROM appointments AS a
INNER JOIN coach_assignments AS ca
  ON a.coach_assignment_id = ca.coach_assignment_id
INNER JOIN specialist_verticals AS sv
  ON ca.specialist_vertical_uuid = sv.specialist_vertical_uuid
WHERE a.complete_at IS NOT NULL