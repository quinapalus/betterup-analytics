WITH src_eu_coach_assignments AS ( 
    SELECT * FROM {{ source('app_eu', 'coach_assignments') }}
),
coach_assignments_eu AS (
    SELECT
    COACH_ID,
    COACH_UNREAD_MESSAGE_COUNT ,
    CREATED_AT,
    ENDED_REASON,
    ID,
    MEMBER_LAST_ACTIVE_AT,
    ROLE,
    UPDATED_AT,
    USER_ID,
    USER_UNREAD_MESSAGE_COUNT         
    FROM src_eu_coach_assignments
)

SELECT * FROM coach_assignments_eu