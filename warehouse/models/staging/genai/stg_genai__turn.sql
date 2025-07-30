WITH src_turn AS (
    SELECT * FROM {{ source('genai', 'turn') }}
),
turn AS (
    SELECT
        id as turn_id,
        conversation_id,
        is_flagged,
        model_prompt,
        model_response,
        user_text,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}

    FROM src_turn
)


SELECT * FROM turn
