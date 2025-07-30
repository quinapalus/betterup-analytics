WITH goals AS (

  SELECT * FROM {{ source('app', 'member_goals') }}

)

SELECT
  id,
  title,
  creator_id,
  assigned_to_id,
  state,
  {{ load_timestamp('state_updated_at') }},
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('complete_by') }},
  {{ load_timestamp('updated_at') }}
FROM goals
