{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH client_notes_development_topics AS (

  SELECT * FROM {{ source('app', 'client_notes_development_topics') }}

)


SELECT
  id AS client_notes_development_topic_id,
  client_note_id,
  development_topic_id
FROM client_notes_development_topics
