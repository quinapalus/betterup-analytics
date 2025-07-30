WITH event_subscription_plan_switch AS(
  SELECT * FROM {{ source('segment_backend', 'subscription_plan_switch') }}
)

SELECT *
FROM event_subscription_plan_switch
