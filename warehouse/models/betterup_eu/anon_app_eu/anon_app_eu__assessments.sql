WITH src_eu_assessments AS (
    SELECT * FROM {{ source('app_eu', 'assessments')}}
),
assessments_eu AS (
    SELECT
        ASSOCIATED_RECORD_ID,
        ASSOCIATED_RECORD_TYPE,
        CREATED_AT::timestamp_ntz,
        CREATOR_ID,
        ID AS assessment_id,
        -- RESPONSES, -- Removed for PII
        SHARED_WITH_COACH,
        SUBMITTED_AT,
        TRACK_ASSIGNMENT_ID,
        TYPE,
        UPDATED_AT::timestamp_ntz,
        USER_ID,
        QUESTIONS_VERSION,
        EXPIRES_AT
    FROM src_eu_assessments
)

SELECT * FROM assessments_eu