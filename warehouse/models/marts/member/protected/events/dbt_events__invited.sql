-- Product subscription assignment invite events that are new or have a lapse in product access only
WITH product_subscription_assignments AS (

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
    psa.product_subscription_assignment_id,
    psa.starts_at AS invited_at,
    COALESCE(psa.ended_at, psa.ends_at) AS ended_at,
    p.product_id,
    p.on_demand,
    p.primary_coaching,
    p.care,
    p.coaching_circles,
    p.workshops,
    p.extended_network
  FROM product_subscription_assignments AS psa
  INNER JOIN product_subscriptions AS ps ON psa.product_subscription_id = ps.product_subscription_id
  INNER JOIN products AS p ON ps.product_id = p.product_id

),

{%- set products = [
  'care',
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
    invited_at AS event_at,
    '{{ product }}_product' AS event_object,
    product_subscription_assignment_id AS associated_record_id,
    product_id,
    ended_at
  FROM join_products
  WHERE {{ product }}
  -- include invite events for a member's first product access, as well as any subsequent
  -- product access that follows lapse in access of more than 24 hours
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY invited_at) = 1 OR
    DATEDIFF(hour, LAG(ended_at) OVER(PARTITION BY member_id ORDER BY invited_at), invited_at) >= 24

),

{%- endfor -%}

unioned AS (

{%- for product in products -%}

  SELECT * FROM {{ product }}_product
  {% if not loop.last %} UNION ALL {% endif %}

{%- endfor -%}

)


SELECT
  -- Surrogate Key of MEMBER_ID, EVENT_ACTION_AND_OBJECT, EVENT_AT
  {{ dbt_utils.surrogate_key(['member_id', 'event_object', 'event_at']) }} AS dbt_events__invited_id,
  member_id,
  event_at,
  'invited' AS event_action,
  event_object,
  event_action || ' ' || event_object AS event_action_and_object,
  'ProductSubscriptionAssignment' AS associated_record_type,
  associated_record_id,
  OBJECT_CONSTRUCT('product_id', product_id, 'ended_at', ended_at) AS attributes
FROM unioned
