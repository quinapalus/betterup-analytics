WITH assessments AS (

  SELECT * FROM {{ ref('stg_wkfw__assessments') }}
  WHERE submitted_at IS NOT NULL

),

-- unnest each item_key/response pair into separate rows
unnest AS (
    SELECT
      a.assessment_id,
      a.user_id,
      r.path AS item_key,
      r.value::STRING AS item_response,
      a.assessment_configuration_id
    FROM assessments AS a
    JOIN LATERAL FLATTEN (input => a.responses) AS r
)

SELECT
  *,
  {{ dbt_utils.surrogate_key(['assessment_id', 'item_key']) }} AS primary_key
FROM unnest