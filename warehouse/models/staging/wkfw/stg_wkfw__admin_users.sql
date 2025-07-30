WITH admin_users AS (

  SELECT * FROM {{ source('wkfw', 'admin_users') }}

)


SELECT
  id AS admin_user_id,
  email,
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM admin_users
