WITH users AS (

  SELECT * FROM {{ source('wkfw', 'users') }}

)


SELECT
  id AS user_id,
  email,
  SPLIT_PART(email, '@', -1) AS email_domain,
  time_zone,
  {{ load_timestamp('accepted_terms_at') }},
  {{ load_timestamp('verified_email_at') }},
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM users
