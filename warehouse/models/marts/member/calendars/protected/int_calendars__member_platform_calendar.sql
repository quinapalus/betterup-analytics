{{
  config(
    tags=['eu']
  )
}}

WITH
track_product__joined AS (
    SELECT * FROM {{ref('track_product__joined')}}
),
member_track_calendar AS (
    SELECT * FROM {{ ref('member_track_calendar') }}
),
member_product_subscription_calendar AS (
    SELECT * FROM {{ ref('member_product_subscription_calendar') }}
),
first_primary_coaching_access AS (

  SELECT
    member_id,
    MIN(lead_activated_at) AS first_primary_coaching_access_date
  FROM track_product__joined
  WHERE primary_coaching
  GROUP BY member_id

),

first_care_access AS (

  SELECT
    member_id,
    MIN(care_activated_at) AS first_care_access_date
  FROM track_product__joined
  WHERE care
  GROUP BY member_id

),

first_foundations_access AS (

  SELECT
    member_id,
    MIN(activated_at) AS first_foundations_access_date
  FROM track_product__joined
  GROUP BY member_id

),

final AS (
  SELECT
    {{ dbt_utils.surrogate_key(['tp.member_id', 'tp.date_key', 'tp.product_subscription_assignment_id']) }} as member_platform_calendar_id,
    tp.date,
    tp.date_key,
    tp.is_last_day_of_month,
    tp.is_day_15_of_month,
    tp.member_id,
    tp.track_assignment_id,
    tp.track_id,
    tp.organization_id,
    tp.product_subscription_assignment_id,
    tp.product_subscription_assignment_id is not null as has_product_subscription,
    tp.product_subscription_id,
    tp.product_id,
    tp.v2,
    tp.psa_starts_at,
    tp.activated_at,
    tp.activated_at is not null as has_member_activated,
    COALESCE(tp.date >= tp.activated_at::date, FALSE) as is_member_activated_on_calendar_date,
    tp.total_days_in_tenure_since_activation,
    tp.lead_activated_at,
    tp.care_activated_at,
    tp.first_invited_to_track_at,
    tp.first_activated_at,
    tp.first_activated_at IS NOT NULL as has_member_ever_activated,
    tp.coaching_cloud,
    tp.on_demand as psa_includes_on_demand,
    tp.primary_coaching as psa_includes_primary_coaching,
    tp.care as psa_includes_care,
    tp.coaching_circles as psa_includes_coaching_circles,
    tp.workshops as psa_includes_workshops,
    tp.extended_network as psa_includes_extended_network,
    {{ sanitize_product_group('on_demand','primary_coaching','care','coaching_circles','workshops','extended_network') }} AS product_group,
    CEIL(DATEDIFF('day', tp.first_activated_at, IFF(tp.date > current_date(), current_date(), tp.date))) + 1 AS days_since_first_activation,
    FLOOR(days_since_first_activation / 30) AS complete_months_since_first_activation,

    -- first primary coaching access fields
    pc.first_primary_coaching_access_date,
    IFF(
        (tp.primary_coaching AND pc.first_primary_coaching_access_date <= current_date() AND date <= current_date()), -- Checks that a member has primary coaching access and that their first date of primary coaching isn't in the future
          CEIL(DATEDIFF('day', pc.first_primary_coaching_access_date, IFF(date > current_date(), current_date(), date))), -- If the end of a member's primary coaching access is in the future, we want to use the current date as the end date for this calculation
          NULL
        ) + 1 AS days_since_primary_coaching_access, -- Adding 1 so that the first day on primary coaching is day 1 instead of day 0
    CEIL(days_since_primary_coaching_access / 30) AS primary_coaching_access_month_number, -- month 1 = days 1-30, month 2 = days 31-90, month 3 = days 91-120, etc
    FLOOR(days_since_primary_coaching_access / 30) AS complete_months_since_primary_coaching_access,

    -- first care access fields
    c.first_care_access_date,
    IFF(
        (tp.care AND c.first_care_access_date <= current_date() AND date <= current_date()), -- Checks that a member has care access and that their first date of care isn't in the future
          CEIL(DATEDIFF('day', c.first_care_access_date, IFF(date > current_date(), current_date(), date))), -- If the end of a member's care access is in the future, we want to use the current date as the end date for this calculation
          NULL
        ) + 1 AS days_since_care_access, -- Adding 1 so that the first day on care is day 1 instead of day 0
    CEIL(days_since_care_access / 30) AS care_access_month_number, -- month 1 = days 1-30, month 2 = days 31-90, month 3 = days 91-120, etc
    FLOOR(days_since_care_access / 30) AS complete_months_since_care_access,

    -- first foundations access fields
    f.first_foundations_access_date,
    IFF(
        (f.first_foundations_access_date <= current_date() AND date <= current_date()), -- Checks that a member has foundations access and that their first date of foundations isn't in the future
          CEIL(DATEDIFF('day', f.first_foundations_access_date, IFF(date > current_date(), current_date(), date))), -- If the end of a member's foundations access is in the future, we want to use the current date as the end date for this calculation
          NULL
        ) + 1 AS days_since_foundations_access, -- Adding 1 so that the first day on foundations is day 1 instead of day 0
    CEIL(days_since_foundations_access / 30) AS foundations_access_month_number, -- month 1 = days 1-30, month 2 = days 31-90, month 3 = days 91-120, etc
    FLOOR(days_since_foundations_access / 30) AS complete_months_since_foundations_access

  FROM track_product__joined AS tp
  LEFT JOIN first_primary_coaching_access AS pc
    ON tp.member_id = pc.member_id
  LEFT JOIN first_care_access AS c
    ON tp.member_id = c.member_id
  LEFT JOIN first_foundations_access AS f
    ON tp.member_id = f.member_id
  WHERE
    -- Logic to only count days during an active product subscription assignment when a member's product subscription assignment ended prior to their track assignment
    -- Historic members not on any product subscription assignment will still be included
    tp.product_subscription_id IS NOT NULL OR
    tp.track_assignment_id NOT IN (SELECT track_assignment_id FROM member_track_calendar AS t INNER JOIN member_product_subscription_calendar AS p ON t.member_id = p.member_id AND t.date = p.date)
)

select *
from final
