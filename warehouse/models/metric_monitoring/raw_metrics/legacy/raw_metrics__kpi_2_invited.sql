with looker_query as (
  WITH event_1 AS (SELECT
              f1.*,
              COALESCE(f2.activated_at < DATE_TRUNC('MINUTE', f1.event_at), FALSE) AS has_previously_activated,
              COALESCE(f3.completed_first_appointment_at < DATE_TRUNC('MINUTE', f1.event_at), FALSE) AS has_previous_appointment,
              TO_CHAR(f1.event_at, 'YYYYMMDD') AS date_key,
              CASE
                WHEN f1.event_name IN ('invited primary_coaching_product','invited on_demand_product','invited extended_network_product', 'invited care_product') AND f1.event_at < '2022-01-01' THEN FALSE
                WHEN f1.event_name IN ('invited primary_coaching_product','invited on_demand_product','invited extended_network_product') AND f1.event_at >= '2022-07-01' THEN TRUE
                WHEN f1.event_name = 'invited care_product' AND f1.event_at >= '2022-02-01' THEN TRUE
                WHEN f1.event_name = 'invited foundations_product' THEN TRUE
                ELSE m.is_on_converged_platform
              END AS is_on_converged_platform
            FROM {{ ref('fact_member_events') }}
      AS f1
            LEFT JOIN (SELECT DISTINCT member_id, MIN(DATE_TRUNC('MINUTE', event_at)) AS activated_at FROM {{ ref('fact_member_events') }}
      WHERE event_action = 'activated' AND associated_record_type IN ('ProductSubscriptionAssignment','TrackAssignment') GROUP BY member_id) AS f2
              ON f1.member_id = f2.member_id
            LEFT JOIN (SELECT DISTINCT member_id, MIN(DATE_TRUNC('MINUTE', event_at)) AS completed_first_appointment_at FROM {{ ref('fact_member_events') }}
      WHERE event_action = 'completed' AND associated_record_type = 'Appointment' GROUP BY member_id) AS f3
              ON f1.member_id = f3.member_id
            INNER JOIN -- if dev -- "ANALYTICS"."CORE"."DIM_MEMBERS"
    {{ ref('dim_members') }}
    AS m
              ON f1.member_id = m.member_id
            WHERE ( f1."EVENT_NAME"  IN ('invited primary_coaching_product', 'invited track')) AND
                  1=1 -- no filter on 'event_1.assessment_type_filter'

              )
    ,  event_2 AS (SELECT f1.primary_key as previous_event_primary_key, f2.*
      FROM event_1 AS f1
      INNER JOIN {{ ref('fact_member_events') }}
      AS f2
      ON
        f1.member_id = f2.member_id AND
        f1.event_at <= f2.event_at
      WHERE ( f2."EVENT_NAME"  IN ('activated primary_coaching_user', 'activated track')) AND
            1=1 -- no filter on 'event_2.assessment_type_filter'

      QUALIFY ROW_NUMBER() OVER (PARTITION BY f1.member_id, f1.associated_record_id ORDER BY f2.event_at) = 1
      )
    ,  track_assignments AS (SELECT
          ta.track_assignment_id,
          ta.track_id,
          ta.member_id,
          ta.created_at,
          ta.ended_at,
          CASE
            -- If member is activated prior to track_assignment creation, mark track_assignment as activated on creation:
            WHEN m.activated_at < ta.created_at THEN ta.created_at
            -- If track_assignment is open, or member activated prior to track_assignment ended, use date member activated (if any):
            WHEN ta.ended_at IS NULL OR m.activated_at < ta.ended_at THEN m.activated_at
            -- In case where member activated after track_assignment ended, track_assignment.activated_at is NULL:
            ELSE NULL
          END AS activated_at,
          -- For members that have multiple track_assignments for a given track, find the first invite date:
          CASE WHEN NOT ta.is_hidden THEN MIN(ta.created_at) OVER (PARTITION BY ta.member_id, ta.track_id, ta.is_hidden) END AS member_first_invited_to_track_at,
          ta.is_hidden,
          ta.is_primary_coaching_enabled,
          ta.is_on_demand_coaching_enabled,
          ta.is_extended_network_coaching_enabled,
          ta.updated_at
        FROM {{ ref('stg_app__track_assignments')}} AS ta
        INNER JOIN -- if dev -- "ANALYTICS"."CORE"."DIM_MEMBERS"
    {{ ref('dim_members') }}
    AS m
          ON ta.member_id = m.member_id
        )
  --Looker link: https://betterup.looker.com/x/HqrbHXljwQxEg1pwPw0Xqc

  SELECT
      (DATE_TRUNC('month', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST(event_1."EVENT_AT"  AS TIMESTAMP_NTZ)))) AS invited_month,
      COUNT(DISTINCT ( event_1."MEMBER_ID"  ) ) AS count_invited,
      COUNT(DISTINCT CASE WHEN (DATEDIFF(day, (event_1."EVENT_AT"), (event_2."EVENT_AT"))  <= 7) THEN ( event_1."MEMBER_ID"  )  ELSE NULL END) AS count_activated_within_7_days
  FROM event_1
  LEFT JOIN event_2 ON (event_1."PRIMARY_KEY") = (event_2."PREVIOUS_EVENT_PRIMARY_KEY")
  LEFT JOIN {{ ref('dim_reporting_group_assignments') }}
      AS reporting_group_assignments ON (event_1."MEMBER_ID") = (reporting_group_assignments."MEMBER_ID") AND
              (event_1."EVENT_AT") >= (reporting_group_assignments."STARTS_AT") AND
              ((event_1."EVENT_AT") < (reporting_group_assignments."ENDED_AT") OR (reporting_group_assignments."ENDED_AT") IS NULL)
  LEFT JOIN track_assignments ON (reporting_group_assignments."ASSOCIATED_ASSIGNMENT_ID") = (track_assignments."TRACK_ASSIGNMENT_ID") AND
              (reporting_group_assignments."ASSOCIATED_RECORD_TYPE") = 'Track'
  INNER JOIN -- if dev -- "ANALYTICS"."CORE"."DIM_TRACKS"
    {{ ref('dim_tracks') }}
    AS tracks ON (track_assignments."TRACK_ID") = (tracks."TRACK_ID")
  LEFT JOIN {{ ref('dim_organizations') }}  AS organizations ON (tracks."ORGANIZATION_ID") = organizations.organization_id
  LEFT JOIN -- if dev -- "ANALYTICS"."ANALYTICS"."MEMBER_PLATFORM_CALENDAR"
    {{ ref('member_platform_calendar') }}
    AS member_platform_calendar ON (event_1."MEMBER_ID") = member_platform_calendar.member_id AND
              (event_1."DATE_KEY") = member_platform_calendar.date_key AND
              (
                ( member_platform_calendar.v2 AND organizations.has_migrated_to_maps ) OR
                ( NOT member_platform_calendar.v2 AND NOT organizations.has_migrated_to_maps ) OR
                ( member_platform_calendar.date <= (TO_CHAR(DATE_TRUNC('second', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST(organizations.v2_psa_enabled_at  AS TIMESTAMP_NTZ))), 'YYYY-MM-DD HH24:MI:SS')) AND NOT member_platform_calendar.v2 ) OR
                ( member_platform_calendar.v2 IS NULL )
              )
  WHERE (NOT (event_1."HAS_PREVIOUSLY_ACTIVATED" ) OR (event_1."HAS_PREVIOUSLY_ACTIVATED" ) IS NULL) AND ((event_1."EVENT_AT" ) >= ((CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', CAST(DATEADD('month', -24, DATE_TRUNC('month', DATE_TRUNC('day', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ))))) AS TIMESTAMP_NTZ)))) AND (member_platform_calendar.primary_coaching )) AND ((NOT (member_platform_calendar.care ) OR (member_platform_calendar.care ) IS NULL) AND ((tracks."DEPLOYMENT_GROUP"  ) = 'B2B / Gov Paid Contract' AND ((CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', CAST(DATE_TRUNC('month', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST(event_1."EVENT_AT"  AS TIMESTAMP_NTZ))) AS TIMESTAMP_NTZ))) < CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', CAST(DATE_TRUNC('month', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST(DATEADD('day', -7, CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ))) AS TIMESTAMP_NTZ)))))
  GROUP BY
      (DATE_TRUNC('month', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST(event_1."EVENT_AT"  AS TIMESTAMP_NTZ))))
  )

select
    looker_query.*, 
    {{ dbt_utils.surrogate_key(['invited_month']) }} as primary_key
from looker_query
