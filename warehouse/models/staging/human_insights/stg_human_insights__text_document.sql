{{
  config(
    materialized='view',
    tags=['eu']
  )
}}

WITH text_scoring_pipeline_text_document AS (
    SELECT * FROM {{ source('human_insights', 'text_scoring_pipeline_text_document') }}
)

SELECT
    id AS document_id,
    text,
    client_key,
    client_correlation_id,
    client_correlation_parent_id,
    num_sentences,
    created_at
FROM text_scoring_pipeline_text_document
