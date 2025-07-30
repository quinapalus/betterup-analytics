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
    u.confirmed_at AS activated_at,
    u.care_confirmed_at,
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
  WHERE (u.confirmed_at IS NOT NULL AND psa.ended_at IS NULL) OR -- checks that a user confirmed on an open psa
        (u.confirmed_at IS NOT NULL AND u.confirmed_at < psa.ended_at) -- checks that a user confirmed on a psa before the psa ended

),

{%- set products = [
  'coaching_circles',
  'extended_network',
  'on_demand',
  'primary_coaching',
  'workshops'
  ]
  -%}


{%- for product in products -%}

{{product}}_product AS (

  SELECT
    member_id,
    CASE
      WHEN activated_at < starts_at THEN starts_at -- If member is activated prior to product_subscription_assignment start, set activated_at (event_at) as product_subscription_assignment.starts_at
      ELSE activated_at
    END AS event_at,
    activated_at < starts_at AS member_activated_prior_to_invitation,
    '{{ product }}_user' AS event_object,
    product_subscription_assignment_id AS associated_record_id
  FROM join_products
  WHERE {{ product }}

),

{%- endfor -%}

care_product AS (

  SELECT
    member_id,
    CASE
      WHEN care_confirmed_at < starts_at THEN starts_at -- If member is activated prior to product_subscription_assignment start, set care_confirmed_at (event_at) as product_subscription_assignment.starts_at
      ELSE care_confirmed_at
    END AS event_at,
    care_confirmed_at < starts_at AS member_activated_prior_to_invitation,
    'care_user' AS event_object,
    product_subscription_assignment_id AS associated_record_id
  FROM join_products
  WHERE care

),

unioned AS (

{%- for product in products -%}

  SELECT * FROM {{ product }}_product
  {% if not loop.last %} UNION ALL {% endif %}

{%- endfor -%}

  UNION ALL SELECT * FROM care_product

)


SELECT
  -- Surrogate Key of Member ID, Event Object, Associated Record ID
  {{ dbt_utils.surrogate_key(['member_id', 'event_object', 'associated_record_id']) }} AS member_object_associated_record_id,
  member_id,
  event_at,
  'activated' AS event_action,
  event_object,
  event_action || ' ' || event_object AS event_action_and_object,
  'ProductSubscriptionAssignment' AS associated_record_type,
  associated_record_id,
  OBJECT_CONSTRUCT('member_activated_prior_to_invitation', member_activated_prior_to_invitation) AS attributes
FROM unioned
