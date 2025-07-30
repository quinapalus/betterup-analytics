{{
  config(
    tags=['eu']
  )
}}

WITH assessment_response_sets AS (

  SELECT * FROM {{ ref('stg_assessment__assessment_response_sets')}}

),

assessment_response_options AS (

  SELECT * FROM {{ ref('stg_assessment__assessment_response_options')}}

),

assessment_response_options_rollup AS (
  -- The point of this rollup CTE is to get the number of possible options in a set,
  -- which is descriptive metadata about an assessment item (ex: item has 11 options)
  SELECT
    assessment_response_set_id,
    COUNT(DISTINCT assessment_response_option_id) AS response_set_number_of_options
  FROM assessment_response_options
  GROUP BY 1

),

final AS (

    SELECT
      ars.assessment_response_set_id,
      ars.key,
      ars.item_type,
      ars.created_at,
      ars.updated_at,
      aro_r.response_set_number_of_options
    FROM assessment_response_sets ars
    LEFT JOIN assessment_response_options_rollup aro_r
      ON ars.assessment_response_set_id = aro_r.assessment_response_set_id

)

SELECT * FROM final

