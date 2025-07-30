WITH program_journey_assignments AS (

  SELECT * FROM {{ source('app', 'program_journey_assignments') }}

)

SELECT
  id AS program_journey_assignment_id,
  user_id,
  program_journey_stage_id,
  {{ load_timestamp('next_stage_at') }},
  {{ load_timestamp('ended_at') }},
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM program_journey_assignments
