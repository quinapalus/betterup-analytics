{{
  config(
    tags=['classification.c3_confidential'],
    materialized='table'
  )
}}

WITH email_bounced AS (
  
  SELECT
    id,
    uuid_ts,
    dispatch_id,
    user_id,
    event,
    campaign_id,
    campaign_name,
    message_variation_id,
    canvas_id,
    canvas_name,
    canvas_variation_id,
    canvas_step_id,
    canvas_step_name,
    bounce_reason,
    null as link_url,
    null as machine_open

  FROM {{source('segment_braze', 'email_bounced')}}

),

email_delivered AS (
  
  SELECT
    id,
    uuid_ts,
    dispatch_id,
    user_id,
    event,
    campaign_id,
    campaign_name,
    message_variation_id,
    canvas_id,
    canvas_name,
    canvas_variation_id,
    canvas_step_id,
    canvas_step_name,
    null as bounce_reason,
    null as link_url,
    null as machine_open

  FROM {{source('segment_braze', 'email_delivered')}}

),

email_link_clicked AS (
  
  SELECT
    id,
    uuid_ts,
    dispatch_id,
    user_id,
    event,
    campaign_id,
    campaign_name,
    message_variation_id,
    canvas_id,
    canvas_name,
    canvas_variation_id,
    canvas_step_id,
    canvas_step_name,
    null as bounce_reason,
    link_url,
    null as machine_open

  FROM {{source('segment_braze', 'email_link_clicked')}}

),

email_marked_as_spam AS (
  
  SELECT
    id,
    uuid_ts,
    dispatch_id,
    user_id,
    event,
    campaign_id,
    campaign_name,
    message_variation_id,
    canvas_id,
    canvas_name,
    canvas_variation_id,
    canvas_step_id,
    canvas_step_name,
    null as bounce_reason,
    null as link_url,
    null as machine_open
    

  FROM {{source('segment_braze', 'email_marked_as_spam')}}

),

email_opened AS (
  
  SELECT
    id,
    uuid_ts,
    dispatch_id,
    user_id,
    event,
    campaign_id,
    campaign_name,
    message_variation_id,
    canvas_id,
    canvas_name,
    canvas_variation_id,
    canvas_step_id,
    canvas_step_name,
    null as bounce_reason,
    null as link_url,
    machine_open

  FROM {{source('segment_braze', 'email_opened')}}

),

email_sent AS (
  
  SELECT
    id,
    uuid_ts,
    dispatch_id,
    user_id,
    event,
    campaign_id,
    campaign_name,
    message_variation_id,
    canvas_id,
    canvas_name,
    canvas_variation_id,
    canvas_step_id,
    canvas_step_name,
    null as bounce_reason,
    null as link_url,
    null as machine_open

  FROM {{source('segment_braze', 'email_sent')}}

),

email_soft_bounced AS (
  
  SELECT
    id,
    uuid_ts,
    dispatch_id,
    user_id,
    event,
    campaign_id,
    campaign_name,
    message_variation_id,
    canvas_id,
    canvas_name,
    canvas_variation_id,
    canvas_step_id,
    canvas_step_name,
    bounce_reason,
    null as link_url,
    null as machine_open

  FROM {{source('segment_braze', 'email_soft_bounced')}}

),

combined_email_events AS (

  SELECT * FROM email_bounced
  UNION
  SELECT * FROM email_delivered
  UNION
  SELECT * FROM email_link_clicked
  UNION
  SELECT * FROM email_marked_as_spam
  UNION
  SELECT * FROM email_opened
  UNION
  SELECT * FROM email_sent
  UNION
  SELECT * FROM email_soft_bounced

)

SELECT 
  id,
  uuid_ts,
  dispatch_id,
  user_id,
  event,
  campaign_id,
  campaign_name,
  message_variation_id,
  canvas_id,
  canvas_name,
  canvas_variation_id,
  canvas_step_id,
  canvas_step_name,
  bounce_reason,
  link_url,
  machine_open
FROM
  combined_email_events
{% if is_incremental() %}
WHERE
  uuid_ts > (SELECT max(uuid_ts) FROM {{this}})
{% endif %}