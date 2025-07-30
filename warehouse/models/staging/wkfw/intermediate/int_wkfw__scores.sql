WITH scores AS (

  SELECT * FROM {{ ref('stg_wkfw__scores') }}

),

assessments AS (

  SELECT * FROM {{ ref('stg_wkfw__assessments') }}

)


SELECT
  s.*,
  a.user_id,
  a.assessment_configuration_id
FROM scores AS s
INNER JOIN assessments AS a
  ON s.assessment_id = a.assessment_id
