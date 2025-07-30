WITH src_eu_appointments AS (
    SELECT * FROM {{ source('app_eu', 'appointments') }}
),

appointments_eu AS (
    SELECT
        CALL_ID,
        COACH_ASSIGNMENT_ID,
        COACH_ID,
        CONTACT_METHOD,
        CREATED_AT,
        CREATOR_ID,
        ENDS_AT,
        ID,
        LENGTH,
        MISSED,
        ORIGINAL_STARTS_AT,
        PARTICIPANT_ID,
        PRODUCT_SUBSCRIPTION_ASSIGNMENT_ID,
        RECURRING,
        REQUESTED_LENGTH,
        SEQUENCE_NUMBER,
        STARTED_AT,
        STARTS_AT,
        TIMESLOT_ID,
        TRACK_ASSIGNMENT_ID,
        UPDATED_AT,
        USER_ID,
        UUID
    FROM src_eu_appointments
)

SELECt * FROM appointments_eu