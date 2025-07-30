WITH program_journey_assignments AS (

  SELECT * FROM {{ ref('stg_app__program_journey_assignments') }}

)


SELECT
  user_id AS member_id,
  created_at AS event_at,
  'assigned' AS event_action,
  'program_journey' AS event_object,
  event_action || ' ' || event_object AS event_action_and_object,
  'ProgramJourneyAssignment' AS associated_record_type,
  program_journey_assignment_id AS associated_record_id,
  OBJECT_CONSTRUCT() AS attributes
FROM program_journey_assignments