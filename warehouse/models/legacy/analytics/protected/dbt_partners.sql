WITH users_global_roles AS (

  SELECT * FROM {{ref('dbt_users_global_roles')}}

)


SELECT
  user_id AS partner_id
FROM users_global_roles
WHERE role = 'partner'
