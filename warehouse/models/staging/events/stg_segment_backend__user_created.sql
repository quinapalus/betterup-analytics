WITH event_user_created AS(
  SELECT *, row_number() over(PARTITION BY user_id ORDER BY created_at) as rn
  FROM {{ source('segment_backend', 'user_created') }}
)

SELECT user_id,
email,
deployment_type,
track_id,
track_assignment_id,
created_at,
platform_name,
platform_version,
device_name,
browser_name,
browser_version,
app_build,
app_version,
product_subscription_assignment_id,
product_subscription_id,
product_id
FROM event_user_created
WHERE rn = 1
