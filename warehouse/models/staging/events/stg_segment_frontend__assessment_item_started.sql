WITH assessment_item_started AS (

  SELECT * FROM {{ source('segment_frontend', 'assessment_item_started') }}

)

SELECT
  id,
  user_id,
  page_name,
  page_url,
  context_page_title,
  context_page_url,
  platform,
  assessment_id,
  item_order_index,
  key,
  event,
  to_timestamp(timestamp) AS event_at
FROM assessment_item_started