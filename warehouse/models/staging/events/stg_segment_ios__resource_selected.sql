WITH resource_selected AS (

  SELECT * FROM {{ source('segment_ios', 'resource_selected') }}
  WHERE
    total_num_resources_loaded is not null
    and resources_ids_in_origin is not null

)


SELECT
  id AS event_id,
  event_text,
  received_at,
  platform,
  user_id,
  resource_id::INT AS resource_id,
  resource_rank,
  resource_origin AS resource_section,
  resources_ids_in_origin AS resources_ids_in_section,
  total_num_resources_loaded AS resources_loaded_count,
  resource_category
FROM resource_selected
