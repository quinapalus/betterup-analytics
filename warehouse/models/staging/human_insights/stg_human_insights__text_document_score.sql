{{
  config(
    materialized='view',
    tags=['eu']
  )
}}

WITH text_scoring_pipeline_text_document_score AS (
    SELECT * FROM {{ source('human_insights', 'text_scoring_pipeline_text_document_score') }}
)

SELECT
    id AS document_score_id,
    text_document_id,
    model,
    label,
    score,
    rank,
    created_at
FROM text_scoring_pipeline_text_document_score
