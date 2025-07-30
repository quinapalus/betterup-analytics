WITH member_goals AS (

  SELECT * FROM {{ ref('stg_app__member_goals') }}

)

SELECT 
  {{ dbt_utils.surrogate_key(['title', 'creator_id','assigned_to_id','created_at']) }} AS primary_key,
  title,
  creator_id,
  assigned_to_id,
  state,
  state_updated_at,
  created_at,
  complete_by,
  updated_at
FROM member_goals

