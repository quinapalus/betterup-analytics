WITH user_notification_preference_changed AS (
  SELECT * FROM {{ source('segment_backend', 'user_notification_preference_changed') }}
)

SELECT
  id,
  saved_changes_email_enabled,
  saved_changes_email_enabled = '[true,false]' as email_unsubscribed,
  saved_changes_email_enabled = '[false,true]' as email_resubscribed,
  timestamp,
  user_id,
  event_text,
  original_timestamp,
  received_at,
  comms_tracking_uuid,
  notification_preference_category
FROM user_notification_preference_changed
ORDER BY received_at DESC
