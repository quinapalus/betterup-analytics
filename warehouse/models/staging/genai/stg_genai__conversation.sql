WITH src_conversation AS (
    SELECT * FROM {{ source('genai', 'conversation') }}
),
conversation AS (
    SELECT
        id as conversation_id,
        context,
        experiment_id,
        initial_message,
        internal_session_id,
        utm_source,
        utm_term,
        version_id,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}

    FROM src_conversation
)


SELECT * FROM conversation
