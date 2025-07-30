WITH product_subscription_assignment_extension_logs AS (

  SELECT * FROM {{ source('app', 'product_subscription_assignment_extension_logs') }}

)

SELECT
  id AS product_subscription_assignment_extension_log_id,
  {{ load_timestamp('created_at') }},
  days_extended,
  product_subscription_assignment_id,
  {{ load_timestamp('updated_at') }},
  user_id
FROM product_subscription_assignment_extension_logs
