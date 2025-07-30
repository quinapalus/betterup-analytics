WITH care_modality_setup_completed AS (

  SELECT * FROM {{ source('segment_backend', 'care_modality_setup_completed') }}

)

SELECT
  id,
  user_id,
  event,
  {{ load_timestamp('timestamp') }}
FROM care_modality_setup_completed