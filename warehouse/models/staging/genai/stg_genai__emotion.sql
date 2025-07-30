WITH src_emotion AS (
    SELECT * FROM {{ source('genai', 'emotion') }}
),
emotion AS (
    SELECT
        id as emotion_id,
        chosen_emotion,
        internal_session_id,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}

    FROM src_emotion
)


SELECT * FROM emotion
