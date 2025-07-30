WITH recommended_item_selected AS (

  SELECT * FROM {{ source('segment_frontend', 'recommended_item_selected') }}

)

SELECT
  id,
  type,
  _id AS item_id,
  section_id,
  position,
  user_id,
  context_locale,
  context_page_referrer,
  context_page_path,
  context_page_title,
  page_url,
  event,
  to_timestamp(timestamp) AS event_at
FROM recommended_item_selected