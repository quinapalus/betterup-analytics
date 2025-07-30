WITH member_goal_actions AS (

  SELECT * FROM {{ ref('stg_app__member_goal_actions') }}

)

SELECT 
  {{ dbt_utils.surrogate_key(['title', 'creator_id','assigned_to_id','created_at']) }} AS primary_key,
  title,
  description,
  creator_id,
  assigned_to_id,
  state,
  state_updated_at,
  created_at,
  complete_by,
  updated_at
FROM member_goal_actions
