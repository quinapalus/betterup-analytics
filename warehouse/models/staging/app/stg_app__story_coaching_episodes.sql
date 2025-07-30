WITH story_coaching_episodes AS (

  SELECT * FROM {{ source('app', 'story_coaching_episodes') }}

)


SELECT
  id AS story_coaching_episode_id,
  resource_id,
  resource_uuid,	
  audio_url,
  image_url,	
  description,	
  duration_milliseconds,	
  episode_number,	
  storyteller,	
  storyteller_title,	
  story_quote,	
  title,
  launch_date,		
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM story_coaching_episodes