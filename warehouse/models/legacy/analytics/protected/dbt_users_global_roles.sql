WITH roles AS (

  SELECT * FROM {{ ref('stg_app__roles') }}

),

users_roles AS (

  SELECT * FROM {{ ref('int_app__users_roles') }}

)


SELECT
  u.user_id,
  r.name AS role
FROM users_roles AS u
INNER JOIN roles AS r
  ON u.role_id = r.role_id AND r.resource_type IS NULL
