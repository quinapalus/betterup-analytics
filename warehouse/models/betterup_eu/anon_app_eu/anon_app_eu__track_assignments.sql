WITH src_eu_track_assignments AS (
    SELECT * FROM {{ source('app_eu', 'track_assignments') }}
),
track_assignments_eu AS (
    SELECT
        CARE,
--        COMMUNITY, --this column does not exist anymore in the postgres DB
        CREATED_AT,
        ENDS_ON,
        EXTENDED_NETWORK,
        HIDDEN,
        ID,
        MINUTES_LIMIT,
        MINUTES_USED,
        ON_DEMAND,
        PEER,
        PRIMARY_COACHING_ENABLED,
        PRIMARY_COACHING_STARTS_AT,
        PROGRAM_CHANGE_NOTIFICATIONS_ENABLED ,
        TRACK_ID,
        UPDATED_AT,
        USER_ID
    FROM src_eu_track_assignments
)

SELECT * FROM track_assignments_eu