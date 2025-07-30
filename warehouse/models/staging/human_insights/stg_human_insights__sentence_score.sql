{{
  config(
    materialized='view',
    tags=['eu']
  )
}}

WITH text_scoring_pipeline_sentence_score AS (
    SELECT * FROM {{ source('human_insights', 'text_scoring_pipeline_sentence_score') }}
)

SELECT
    id AS sentence_score_id,
    sentence_id,
    model,
    label,
    score,
    rank,
    created_at
FROM text_scoring_pipeline_sentence_score
