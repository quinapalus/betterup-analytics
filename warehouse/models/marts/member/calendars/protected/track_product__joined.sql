WITH member_track_calendar AS (

  SELECT * FROM {{ref('member_track_calendar')}}

),

member_product_subscription_calendar AS (

  SELECT * FROM {{ref('member_product_subscription_calendar')}}

),

join_track_product AS (

  SELECT
    COALESCE(t.date, p.date) AS date,
    to_char(COALESCE(t.date, p.date), 'YYYYMMDD') AS date_key,
    COALESCE(t.date, p.date) = LAST_DAY(COALESCE(t.date, p.date)) AS is_last_day_of_month,
    date_part(day, COALESCE(t.date, p.date)) = 15 as is_day_15_of_month,
    COALESCE(t.member_id, p.member_id) AS member_id,
    t.track_assignment_id,
    t.track_id,
    p.organization_id,
    p.product_subscription_assignment_id,
    p.starts_at AS psa_starts_at,
    p.product_subscription_id,
    p.product_id,
    p.v2,
    t.activated_at,
    t.total_days_in_tenure_since_activation,
    t.lead_activated_at,
    t.care_activated_at,
    t.member_first_invited_to_track_at AS first_invited_to_track_at,
    MIN(t.activated_at) OVER(PARTITION BY COALESCE(t.member_id, p.member_id)) AS first_activated_at,
    p.coaching_cloud,
    COALESCE(p.on_demand, t.is_on_demand_coaching_enabled) as on_demand,
    COALESCE(p.primary_coaching, t.is_primary_coaching_enabled) as primary_coaching,
    COALESCE(p.care,t.is_care_coaching_enabled) as care,
    COALESCE(p.coaching_circles,'false') as coaching_circles,
    COALESCE(p.workshops,'false') as workshops,
    COALESCE(p.extended_network,t.is_extended_network_coaching_enabled) as extended_network
  FROM member_track_calendar AS t
  LEFT JOIN member_product_subscription_calendar AS p
    ON t.member_id = p.member_id AND
       t.date = p.date

),

final as (
  
  select {{ dbt_utils.surrogate_key(['member_id', 'date_key', 'product_subscription_assignment_id']) }} as track_product_joined_id,
  *
  from join_track_product
)

select *
from final
