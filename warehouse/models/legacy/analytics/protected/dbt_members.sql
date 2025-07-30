WITH users_global_roles AS (

  SELECT * FROM {{ref('dbt_users_global_roles')}}

),

open_track_assignments AS (

  SELECT * FROM {{ref('stg_app__track_assignments')}}
  WHERE ended_at IS NULL

)


SELECT
  user_id AS member_id,
  ota.member_id IS NOT NULL AS is_current_member
FROM users_global_roles AS m
LEFT OUTER JOIN open_track_assignments AS ota
  ON m.user_id = ota.member_id
WHERE m.role = 'member'
