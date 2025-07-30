WITH assessments AS (

  SELECT * FROM {{ ref('stg_app__assessments') }}

),

-- unnest each item_key/response pair into separate rows
unnest AS (
    SELECT
      {{ dbt_utils.surrogate_key(['a.assessment_id', 'r.path']) }} as primary_key,
      a.assessment_id,
      a.type,
      a.questions_version,
      r.path AS item_key,
      r.value::STRING AS item_response,
      a.assessment_configuration_id,
      a.assessment_configuration_uuid
    FROM assessments AS a
    JOIN LATERAL FLATTEN (input => a.responses) AS r
)

SELECT * FROM unnest