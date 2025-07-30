WITH src_eu_billable_events AS (
    SELECT * FROM {{ source('app_eu', 'billable_events')}}
),
billable_events_eu AS (
    SELECT
    AMOUNT_DUE,
    ASSOCIATED_RECORD_ID,
    ASSOCIATED_RECORD_TYPE,
    COACHING_CLOUD,
    COACHING_TYPE,
    COACH_ID,
    COACH_PROFILE_PAY_RATE_ID,
    CREATED_AT,
    CURRENCY_CODE,
    EVENT_AT,
    EVENT_TYPE,
    ID,
    NOTES,
    UNITS,
    UPDATED_AT,
    USAGE_MINUTES
    FROM src_eu_billable_events
)

SELECT * FROM billable_events_eu