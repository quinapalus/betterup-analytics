with looker_query as (
    --Looker link: https://betterup.looker.com/x/WmYfFxGCQwwcrqsM982x8J

    SELECT
        DATE_TRUNC('month', member_platform_calendar.date ) AS calendar_month,
        COALESCE(CAST( ( SUM(DISTINCT (CAST(FLOOR(COALESCE(CASE WHEN  (member_engagement_events.eventable_type = 'Appointment') AND (member_engagement_events.verb = 'completed') AND ((sessions."COACH_TYPE"  IN ('primary', 'secondary', 'on_demand')))  THEN  session_time.usage_minutes   / 60   ELSE NULL END
    ,0)*(1000000*1.0)) AS DECIMAL(38,0))) + (TO_NUMBER(MD5(CASE WHEN  (member_engagement_events.eventable_type = 'Appointment') AND (member_engagement_events.verb = 'completed') AND ((sessions."COACH_TYPE"  IN ('primary', 'secondary', 'on_demand')))  THEN  session_time.billable_event_id ||  session_time.member_id   ELSE NULL END
    ), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0) ) - SUM(DISTINCT (TO_NUMBER(MD5(CASE WHEN  (member_engagement_events.eventable_type = 'Appointment') AND (member_engagement_events.verb = 'completed') AND ((sessions."COACH_TYPE"  IN ('primary', 'secondary', 'on_demand')))  THEN  session_time.billable_event_id ||  session_time.member_id   ELSE NULL END
    ), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') % 1.0e27)::NUMERIC(38, 0)) )  AS DOUBLE PRECISION) / CAST((1000000*1.0) AS DOUBLE PRECISION), 0) AS session_hours,
        COUNT(DISTINCT CASE WHEN (member_engagement_events.eventable_type = 'Appointment') AND (member_engagement_events.verb = 'completed') AND ((sessions."COACH_TYPE"  IN ('primary', 'secondary', 'on_demand'))) THEN member_engagement_events.engagement_event_id  ELSE NULL END) AS count_coaching_sessions,
        COUNT(DISTINCT CASE WHEN (member_engagement_events.eventable_type = 'Appointment') AND (member_engagement_events.verb = 'completed') AND ((sessions."COACH_TYPE"  IN ('on_demand', 'primary', 'secondary'))) THEN member_platform_calendar.member_id  ELSE NULL END) AS members_with_coaching_session,
        COUNT(DISTINCT CASE WHEN member_platform_calendar.is_day_15_of_month  THEN member_platform_calendar.member_id  ELSE NULL END) AS mid_month_members
    FROM -- if dev -- "ANALYTICS"."ANALYTICS"."MEMBER_PLATFORM_CALENDAR"
    {{ ref('member_platform_calendar') }}
    AS member_platform_calendar
    LEFT JOIN {{ ref('member_engagement_events') }}  AS member_engagement_events ON member_platform_calendar.date_key = member_engagement_events.date_key AND
        member_platform_calendar.member_id = member_engagement_events.user_id
    LEFT JOIN {{ ref('sessions') }}
    AS sessions ON member_engagement_events.user_id = (sessions."MEMBER_ID") AND member_engagement_events.eventable_id = (sessions."SESSION_ID") AND member_engagement_events.eventable_type = 'Appointment'
    LEFT JOIN -- if dev -- "ANALYTICS"."CORE"."DIM_TRACKS"
    {{ ref('dim_tracks') }}
    AS tracks ON member_platform_calendar.track_id = (tracks."TRACK_ID")
    LEFT JOIN {{ ref('dim_organizations') }}  AS organizations ON (tracks."ORGANIZATION_ID") = organizations.organization_id
    LEFT JOIN {{ ref('session_time') }}  AS session_time ON member_engagement_events.eventable_id = session_time.eventable_id_join
                AND member_engagement_events.eventable_type = session_time.eventable_type_join
                AND member_engagement_events.user_id = session_time.member_id
                AND member_engagement_events.verb = session_time.verb_join
    WHERE ((( member_platform_calendar.date  ) >= ((DATEADD('month', -24, DATE_TRUNC('month', DATE_TRUNC('day', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ))))))) AND ( member_platform_calendar.date  ) < ((DATEADD('month', 24, DATEADD('month', -24, DATE_TRUNC('month', DATE_TRUNC('day', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST(CURRENT_TIMESTAMP() AS TIMESTAMP_NTZ)))))))))) AND ((member_platform_calendar.is_member_activated_on_calendar_date ) AND (member_platform_calendar.primary_coaching )) AND ((NOT (member_platform_calendar.care ) OR (member_platform_calendar.care ) IS NULL) AND ((tracks."DEPLOYMENT_GROUP"  ) = 'B2B / Gov Paid Contract' AND (( member_platform_calendar.v2 AND organizations.has_migrated_to_maps ) OR
                        ( NOT member_platform_calendar.v2 AND (NOT organizations.has_migrated_to_maps OR organizations.has_migrated_to_maps IS NULL) ) OR
                        ( member_platform_calendar.date <= (TO_CHAR(DATE_TRUNC('second', CONVERT_TIMEZONE('UTC', 'America/Los_Angeles', CAST(organizations.v2_psa_enabled_at  AS TIMESTAMP_NTZ))), 'YYYY-MM-DD HH24:MI:SS')) AND NOT member_platform_calendar.v2 ) OR
                        ( member_platform_calendar.v2 IS NULL ) )))
    GROUP BY
        (DATE_TRUNC('month', member_platform_calendar.date ))
    ORDER BY
        1 DESC)

select 
    looker_query.*,
    {{ dbt_utils.surrogate_key(['calendar_month']) }} as primary_key
from looker_query
