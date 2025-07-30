{{
  config(
    materialized='table',
    tags=['eu']
  )
}}

WITH doc_scores AS (
    SELECT
        a.document_id AS id,
        a.text,
        a.client_correlation_id,
        a.client_correlation_parent_id,
        a.created_at,
        b.model,
        b.label,
        b.score,
        b.rank
    FROM {{ ref('stg_human_insights__text_document') }} a
    JOIN {{ ref('stg_human_insights__text_document_score') }} b
        ON a.document_id = b.text_document_id
    WHERE REGEXP_LIKE(a.client_correlation_parent_id, 'bu_app_(us|eu)_assessments')
  ),

sentiment_score AS (
    SELECT id, label AS sentiment, score AS sentiment_score
    FROM   doc_scores
    WHERE  model = 'sentiment' AND rank = 1
  ),

emotion_scores AS (
    SELECT
        s1.id,
        s1.emotion_1,
        s1.emotion_score_1,
        s2.emotion_2,
        s2.emotion_score_2,
        s3.emotion_3,
        s3.emotion_score_3
    FROM (
        SELECT id, label AS emotion_1, score AS emotion_score_1
        FROM   doc_scores
        WHERE  model = 'emotion' AND rank = 1
    ) AS s1
    LEFT JOIN (
        SELECT id, label AS emotion_2, score AS emotion_score_2
        FROM   doc_scores
        WHERE  model = 'emotion' AND rank = 2
    ) AS s2
        ON s1.id = s2.id
    LEFT JOIN (
        SELECT id, label AS emotion_3, score AS emotion_score_3
        FROM   doc_scores
        WHERE  model = 'emotion' AND rank = 3
    ) AS s3
        ON s1.id = s3.id
  ),

theme_scores AS (
    SELECT
        s1.id,
        s1.theme_1,
        s1.theme_score_1,
        s2.theme_2,
        s2.theme_score_2,
        s3.theme_3,
        s3.theme_score_3
    FROM (
        SELECT id, label AS theme_1, score AS theme_score_1
        FROM   doc_scores
        WHERE  model = 'theme' AND rank = 1
    ) AS s1
    LEFT JOIN (
        SELECT id, label AS theme_2, score AS theme_score_2
        FROM   doc_scores
        WHERE  model = 'theme' AND rank = 2
    ) AS s2
        ON s1.id = s2.id
    LEFT JOIN (
        SELECT id, label AS theme_3, score AS theme_score_3
        FROM   doc_scores
        WHERE  model = 'theme' AND rank = 3
    ) AS s3
        ON s1.id = s3.id
  ),

informative_score AS (
    SELECT id, label AS information_content, score AS information_content_score
    FROM   doc_scores
    WHERE  model = 'informative' AND rank = 1
  ),

wle_score AS (
    SELECT id, label AS work_life_event, score AS work_life_event_score
    FROM   doc_scores
    WHERE  model = 'work_life_event' AND rank = 1
  ),

item_key_attr AS (
    SELECT b.text_document_id, b.value_string AS item_key
    FROM   {{ ref('stg_human_insights__text_document') }} a
    LEFT JOIN {{ ref('stg_human_insights__document_attributes') }} b
        ON a.document_id = b.text_document_id
    WHERE b.attribute_name = 'item key'
        AND REGEXP_LIKE(a.client_correlation_parent_id, 'bu_app_(us|eu)_assessments')
  ),

submit_time_attr AS (
    SELECT b.text_document_id, b.value_datetime AS scored_datetime
    FROM   {{ ref('stg_human_insights__text_document') }} a
    LEFT JOIN {{ ref('stg_human_insights__document_attributes') }} b
        ON a.document_id = b.text_document_id
    WHERE b.attribute_name = 'submit time'
        AND REGEXP_LIKE(a.client_correlation_parent_id, 'bu_app_(us|eu)_assessments')
  ),

assessment_scores AS (
    SELECT DISTINCT d.id,
                    sta.scored_datetime,
                    d.client_correlation_id AS assessment_id,
                    ika.item_key,
                    d.text                  AS item_response,
                    s.sentiment,
                    s.sentiment_score,
                    e.emotion_1,
                    e.emotion_score_1,
                    e.emotion_2,
                    e.emotion_score_2,
                    e.emotion_3,
                    e.emotion_score_3,
                    t.theme_1,
                    t.theme_score_1,
                    t.theme_2,
                    t.theme_score_2,
                    t.theme_3,
                    t.theme_score_3,
                    i.information_content,
                    i.information_content_score,
                    w.work_life_event,
                    w.work_life_event_score
    FROM (
        SELECT
            document_id AS id,
            client_correlation_id,
            client_correlation_parent_id,
            text,
            created_at
        FROM  {{ ref('stg_human_insights__text_document') }}
        WHERE REGEXP_LIKE(client_correlation_parent_id, 'bu_app_(us|eu)_assessments')
    ) d
    JOIN item_key_attr ika          ON ika.text_document_id = d.id
    JOIN submit_time_attr sta       ON sta.text_document_id = d.id
    LEFT JOIN sentiment_score s     ON s.id = d.id
    LEFT JOIN emotion_scores e      ON e.id = d.id
    LEFT JOIN theme_scores t        ON t.id = d.id
    LEFT JOIN informative_score i   ON i.id = d.id
    LEFT JOIN wle_score w           ON w.id = d.id
  )

SELECT * FROM assessment_scores
