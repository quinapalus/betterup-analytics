{{
  config(
    tags=['classification.c3_confidential','eu'],
    materialized='table'
  )
}}

-- This model uses session starts_at date for Appointment completed events instead of the event date in the engagement events table
-- This logic is being done because the event date in the engagement events table is the date when the coach marked the session completed, not the actual session date

WITH engagement_events AS (
  SELECT 
    engagement_event_id,
    date,
    date_key,
    user_id,
    eventable_id,
    eventable_type,
    verb,
    eventable_subject,
    event_at
  FROM {{ref('stg_app__engagement_events')}}
),

appointments AS (
  SELECT * FROM {{ref('stg_app__appointments')}}
),

engagement_events_filtered AS (
  SELECT * FROM engagement_events
  WHERE EVENTABLE_TYPE || VERB != 'Appointmentcompleted'
),

engagement_events_completed_appointments AS (
  SELECT
    e.ENGAGEMENT_EVENT_ID,
    TO_DATE(a.STARTS_AT) AS DATE,
    TO_CHAR(a.STARTS_AT, 'YYYYMMDD') AS DATE_KEY,
    e.USER_ID,
    e.EVENTABLE_ID,
    e.EVENTABLE_TYPE,
    e.VERB,
    e.EVENTABLE_SUBJECT,
    a.STARTS_AT AS EVENT_AT
  FROM engagement_events AS e
  INNER JOIN appointments AS a
    ON e.EVENTABLE_ID = a.APPOINTMENT_ID
  WHERE e.EVENTABLE_TYPE = 'Appointment'
  AND e.VERB = 'completed'
),

unioned AS (
  SELECT * FROM engagement_events_filtered
  UNION ALL SELECT * FROM engagement_events_completed_appointments
)

SELECT
  *,
  ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY event_at) AS event_sequence
FROM unioned
