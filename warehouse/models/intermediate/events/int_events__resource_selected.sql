WITH
web_selected_resources AS (

  SELECT * FROM {{ref('stg_segment_frontend__resource_selected')}}

),

ios_selected_resources AS (

  SELECT * FROM {{ref('stg_segment_ios__resource_selected')}}

),

android_selected_resources AS (

  SELECT * FROM {{ref('stg_segment_android__resource_selected')}}

)

{% set columns = 'event_id, event_text, received_at, platform, user_id, resource_id, resource_section, resources_ids_in_section, resource_rank, resources_loaded_count, resource_category' %}

SELECT {{ columns }} FROM web_selected_resources
UNION ALL
SELECT {{ columns }} FROM ios_selected_resources
UNION ALL
SELECT {{ columns }} FROM android_selected_resources
ORDER BY received_at DESC
