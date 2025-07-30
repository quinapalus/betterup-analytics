{{
  config(
    materialized='view',
    tags=['eu']
  )
}}

WITH text_scoring_pipeline_attribute AS (
    SELECT * FROM {{ source('human_insights', 'text_scoring_pipeline_attribute') }}
)

SELECT
    id AS attribute_id,
    text_document_id,
    attribute_name,
    attribute_type,
    value_string,
    value_float,
    value_datetime,
    created_at
FROM text_scoring_pipeline_attribute
