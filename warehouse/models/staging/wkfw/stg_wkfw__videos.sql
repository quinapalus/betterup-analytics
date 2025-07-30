WITH videos AS (

  SELECT * FROM {{ source('wkfw', 'videos') }}

)


SELECT
  id AS video_id,
  vimeo_id,
  title,
  description,
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }},
  {{ load_timestamp('publish_date') }}
FROM videos
