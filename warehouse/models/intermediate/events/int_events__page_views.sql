{{
  config(
    tags=['eu']
  )
}}

WITH web_views AS (

  SELECT * FROM {{ref('stg_segment_frontend__page_views')}}

),

ios_views AS (

  SELECT * FROM {{ref('stg_segment_ios__page_views')}}

),

android_views AS (

  SELECT * FROM {{ref('stg_segment_android__page_views')}}

)

{% set columns = 'event_id, user_id, event_text, received_at, page_name, page_url, platform, user_agent' %}

SELECT {{ columns }} FROM web_views
UNION
SELECT {{ columns }} FROM ios_views
UNION
SELECT {{ columns }} FROM android_views
ORDER BY received_at DESC
