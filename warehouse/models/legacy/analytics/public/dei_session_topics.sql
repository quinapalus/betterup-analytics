WITH session_topics AS (

  SELECT * FROM {{ref('dbt_session_topics')}}

),

billable_events AS (

  SELECT * FROM {{ref('app_billable_events')}}

),

tracks AS (

  SELECT * FROM {{ref('dim_tracks')}} 
  WHERE is_external and engaged_member_count is not null --this logic was in dei_tracks which this model used to reference

),

topic_mapping AS (

  SELECT * FROM {{ref('bu_topic_mapping')}}

),

sessions AS (

  SELECT * FROM (

  SELECT
    associated_record_id AS session_id,
    event_at AS session_at,
    track_id,
    ROW_NUMBER() OVER (
        PARTITION BY associated_record_id
        ORDER BY associated_record_id, event_at DESC
    ) AS index
  FROM billable_events
  WHERE associated_record_type = 'Session'
    AND event_type = 'completed_sessions'
    -- limit sessions to tracks included in tracks where is_external, ie external deployments
    AND track_id IN (SELECT track_id FROM tracks)
    
  ) a
  
  WHERE index = 1

)


SELECT
  s.session_id,
  st.member_id,
  st.coach_id,
  s.track_id,
  s.session_at,
  st.development_topic_id,
  st.topic,
  st.theme,
  tm.category AS wpm_dimension,
  tm.theme AS wpm_factor,
  st.session_topics_count
FROM sessions AS s
INNER JOIN session_topics AS st ON s.session_id = st.session_id
INNER JOIN topic_mapping AS tm ON tm.mapping = 'BetterUp - Whole Person'
  AND st.topic = tm.topic
