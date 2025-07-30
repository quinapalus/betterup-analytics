{{
  config(
    materialized='view',
    tags=['eu']
  )
}}

WITH text_scoring_pipeline_sentence AS (
    SELECT * FROM {{ source('human_insights', 'text_scoring_pipeline_sentence') }}
)

SELECT
    id AS sentence_id,
    text_document_id,
    sentence_index,
    sentence_len,
    text,
    created_at
FROM text_scoring_pipeline_sentence
