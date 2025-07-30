--Looker link: https://betterup.looker.com/x/UwqHt48IsQBMKA2gQMku0Y
with looker_query as (
  WITH engagement_events AS (SELECT
          *
        FROM {{ source('app', 'engagement_events') }}
        WHERE NOT (eventable_type = 'RatedResource' AND event_at > '02/03/2020' AND event_at < '02/05/2020')
        )
    ,  member_daily_engagement AS (SELECT
          user_id AS member_id,
          to_char(event_at, 'YYYYMMDD') AS date_key,
          COUNT(*) AS engagement_event_count,
          COUNT(*) > 0 AS member_was_engaged_on_day
        FROM engagement_events
        GROUP BY user_id, date_key
      )
    ,  primary_coaching_engagement_denormalized AS (WITH oneonone_sessions AS (
          SELECT
            member_id,
            to_char(event_at, 'YYYYMMDD') AS date_key,
            COUNT(billable_event_id) daily_1on1_sessions,
            COUNT(billable_event_id) > 0 had_1on1_sesion_on_day
          FROM {{ ref('app_billable_events') }}  be
          -- ensures event was "coaching" and completed
          WHERE event_type = 'completed_sessions'
          -- identifies 1on1 coaching
          AND coaching_type IN ('primary','secondary','on_demand')
          GROUP BY 1,2 )

          SELECT
            c.*,
            e.engagement_event_count,
            e.member_was_engaged_on_day,
            s.had_1on1_sesion_on_day,
            MAX(complete_months_since_primary_coaching_access) OVER(PARTITION BY c.member_id) AS max_complete_months_since_primary_coaching_access
          FROM -- if dev -- "ANALYTICS"."ANALYTICS"."MEMBER_PLATFORM_CALENDAR"
    {{ ref('member_platform_calendar') }}
    c
          LEFT JOIN member_daily_engagement e ON c.member_id = e.member_id AND c.date_key = e.date_key
          LEFT JOIN oneonone_sessions AS s ON s.member_id = e.member_id AND s.date_key = e.date_key
          WHERE primary_coaching )
  SELECT
      (DATE_TRUNC('month', primary_coaching_engagement_denormalized.first_primary_coaching_access_date )) AS first_primary_coaching_access_month,
      COUNT(DISTINCT CASE WHEN (primary_coaching_engagement_denormalized.days_since_primary_coaching_access  = 105) AND ((datediff(month, (TO_CHAR(TO_DATE(primary_coaching_engagement_denormalized.first_primary_coaching_access_date ), 'YYYY-MM-DD')), current_date())  >= 5)) THEN primary_coaching_engagement_denormalized.member_id  ELSE NULL END) AS count_members_primary_coaching_access_4_months,
      COUNT(DISTINCT CASE WHEN primary_coaching_engagement_denormalized.member_was_engaged_on_day AND (primary_coaching_engagement_denormalized.primary_coaching_access_month_number  = 4) AND (primary_coaching_engagement_denormalized.max_complete_months_since_primary_coaching_access  >= 4) AND ((member_engagement_events.eventable_type IS NOT NULL)) THEN primary_coaching_engagement_denormalized.member_id  ELSE NULL END) AS members_w_1_eng_in_fourth_month_req_4_months
  FROM -- if dev -- "ANALYTICS"."ANALYTICS"."MEMBER_PLATFORM_CALENDAR"
    {{ ref('member_platform_calendar') }}
    AS member_platform_calendar
  LEFT JOIN {{ ref('member_engagement_events') }}  AS member_engagement_events ON member_platform_calendar.date_key = member_engagement_events.date_key AND
        member_platform_calendar.member_id = member_engagement_events.user_id
  LEFT JOIN -- if dev -- "ANALYTICS"."CORE"."DIM_TRACKS"
    {{ ref('dim_tracks') }}
    AS tracks ON member_platform_calendar.track_id = (tracks."TRACK_ID")
  LEFT JOIN {{ ref('dim_organizations') }}  AS organizations ON (tracks."ORGANIZATION_ID") = organizations.organization_id
  LEFT JOIN primary_coaching_engagement_denormalized ON member_platform_calendar.date_key = primary_coaching_engagement_denormalized.date_key AND member_platform_calendar.member_id = primary_coaching_engagement_denormalized.member_id
  WHERE (member_platform_calendar.is_member_activated_on_calendar_date ) AND ((member_platform_calendar.primary_coaching ) AND (NOT (member_platform_calendar.care ) OR (member_platform_calendar.care ) IS NULL)) AND ((tracks."DEPLOYMENT_GROUP"  ) = 'B2B / Gov Paid Contract' AND (((( primary_coaching_engagement_denormalized.first_primary_coaching_access_date  ) >= ((DATEADD('month', -28, DATE_TRUNC('month', DATE_TRUNC('day', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ))))))) AND ( primary_coaching_engagement_denormalized.first_primary_coaching_access_date  ) < ((DATEADD('month', 24, DATEADD('month', -28, DATE_TRUNC('month', DATE_TRUNC('day', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ)))))))))) AND (( member_platform_calendar.v2 AND organizations.has_migrated_to_maps ) OR
                        ( NOT member_platform_calendar.v2 AND (NOT organizations.has_migrated_to_maps OR organizations.has_migrated_to_maps IS NULL) ) OR
                        ( member_platform_calendar.date <= (TO_CHAR(DATE_TRUNC('second', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST(organizations.v2_psa_enabled_at  AS TIMESTAMP_NTZ))), 'YYYY-MM-DD HH24:MI:SS')) AND NOT member_platform_calendar.v2 ) OR
                        ( member_platform_calendar.v2 IS NULL ) )))
  GROUP BY
      (DATE_TRUNC('month', primary_coaching_engagement_denormalized.first_primary_coaching_access_date ))
  )
select
    looker_query.*, 
    {{ dbt_utils.surrogate_key(['first_primary_coaching_access_month']) }} as primary_key
from looker_query
