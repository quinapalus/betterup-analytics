WITH users AS (

  SELECT * FROM  {{ ref('int_app__users') }}

),

product_subscription_assignments AS (

  SELECT * FROM {{ ref('int_app__product_subscription_assignments') }}
  WHERE starts_at <= CURRENT_TIMESTAMP

),

product_subscriptions AS (

  SELECT * FROM {{ ref('stg_app__product_subscriptions') }}

),

products AS (

  SELECT * FROM {{ ref('stg_app__products') }}

),

join_products AS (

  SELECT
    psa.member_id,
    u.lead_confirmed_at,
    psa.product_subscription_assignment_id,
    psa.starts_at,
    psa.ended_at,
    p.on_demand,
    p.primary_coaching,
    p.care,
    p.coaching_circles,
    p.workshops,
    p.extended_network
  FROM users AS u
  INNER JOIN product_subscription_assignments AS psa ON u.user_id = psa.member_id
  INNER JOIN product_subscriptions AS ps ON psa.product_subscription_id = ps.product_subscription_id
  INNER JOIN products AS p ON ps.product_id = p.product_id
  WHERE (u.lead_confirmed_at IS NOT NULL AND psa.ended_at IS NULL) OR -- checks that a user confirmed on an open psa
        (u.lead_confirmed_at IS NOT NULL AND u.lead_confirmed_at < psa.ended_at) -- checks that a user confirmed on a psa before the psa ended

),

lead_product AS (

  SELECT
    member_id,
    CASE
      WHEN lead_confirmed_at < starts_at THEN starts_at -- If member is activated prior to product_subscription_assignment start, set activated_at (event_at) as product_subscription_assignment.starts_at
      ELSE lead_confirmed_at
    END AS event_at,
    lead_confirmed_at < starts_at AS member_activated_prior_to_invitation,
    'lead_product' AS event_object,
    product_subscription_assignment_id AS associated_record_id
  FROM join_products
  WHERE ((primary_coaching OR on_demand) OR (extended_network AND NOT care))

)


SELECT
  {{ dbt_utils.surrogate_key(['member_id', 'event_object', 'associated_record_id']) }} AS member_object_associated_record_id,
  member_id,
  event_at,
  'activated' AS event_action,
  event_object,
  event_action || ' ' || event_object AS event_action_and_object,
  'ProductSubscriptionAssignment' AS associated_record_type,
  associated_record_id,
  OBJECT_CONSTRUCT('member_activated_prior_to_invitation', member_activated_prior_to_invitation) AS attributes
FROM lead_product
