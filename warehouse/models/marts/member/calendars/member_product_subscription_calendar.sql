WITH dim_date AS (
  SELECT * FROM {{ref('dim_date')}}
),

product_subscription_assignments AS (
  SELECT * FROM  {{ref('stg_app__product_subscription_assignments')}}
),

product_subscriptions AS (
  SELECT * FROM  {{ref('stg_app__product_subscriptions')}}
),

products AS (
  SELECT * FROM  {{ref('stg_app__products')}}
),

join_products AS (
  SELECT
    ps.product_subscription_id,
    ps.organization_id,
    p.product_id,
    p.on_demand,
    p.primary_coaching,
    p.care,
    p.coaching_circles,
    p.workshops,
    p.extended_network,
    p.coaching_cloud
  FROM product_subscriptions AS ps
  INNER JOIN products AS p
  ON ps.product_id = p.product_id
),

joined AS (
  SELECT
    c.date_key,
    c.date,
    c.calendar_year_month,
    c.is_current_fiscal_quarter,
    c.is_previous_fiscal_quarter,
    c.date = LAST_DAY(c.date) AS is_last_day_of_month,
    p.product_subscription_assignment_id,
    p.member_id,
    p.v2,
    DATE_TRUNC('day', p.starts_at) AS starts_date,
    starts_at,
    DATE_TRUNC('day', p.ended_at) AS ended_date,
    c.date >= date_trunc('day', p.starts_at) AS member_was_active_on_product_subscription,
    IFF(member_was_active_on_product_subscription, CEIL(DATEDIFF('second', p.starts_at, c.date) / 86400.0), NULL) AS days_on_product_subscription,
    IFF(member_was_active_on_product_subscription, CEIL(days_on_product_subscription / 30.0), NULL) AS months_on_product_subscription,
    ROW_NUMBER() OVER (PARTITION BY date, member_id, v2 ORDER BY starts_at desc) as row_member_date_version,
    join_products.*
  FROM dim_date AS c
  INNER JOIN product_subscription_assignments AS p
    ON c.date >= DATE_TRUNC('day', p.starts_at) AND
       c.date < COALESCE(DATE_TRUNC('day', p.ended_at), DATE_TRUNC('day', p.ends_at), DATEADD(DAY, 365, CURRENT_DATE()))
  INNER JOIN join_products USING(product_subscription_id)
),

final AS (
  SELECT {{ dbt_utils.surrogate_key(['member_id', 'date_key', 'product_subscription_assignment_id']) }} as member_product_subscription_calendar_id,
  * 
  FROM joined
  -- If not V2, limits to unique record per member per day, and uses the last PSA a member was on in a day
  WHERE iff(v2, 1=1, row_member_date_version=1)
)

SELECT *
FROM final
