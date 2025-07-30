WITH goal_actions AS (

  SELECT * FROM {{ source('app', 'member_goal_actions') }}

)

SELECT
  id,
  title,
  description,
  creator_id,
  assigned_to_id,
  state,
  {{ load_timestamp('state_updated_at') }},
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('complete_by') }},
  {{ load_timestamp('updated_at') }}
FROM goal_actions
