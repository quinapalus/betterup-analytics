WITH scores AS (

  SELECT * FROM {{ source('wkfw', 'scores') }}

)


SELECT
  id AS score_id,
  assessment_id,
  construct_id,
  raw_score,
  scale_score,
  z_score,
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM scores
