{{
  config(
    materialized='view',
    tags=['eu']
  )
}}

WITH text_scoring_pipeline_term AS (
    SELECT * FROM {{ source('human_insights', 'text_scoring_pipeline_term') }}
)

SELECT
    id AS term_id,
    text_document_id,
    sentence_id,
    term,
    term_type,
    topic,
    phrase,
    created_at
FROM text_scoring_pipeline_term
