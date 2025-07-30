{{
  config(
    tags=['eu']
  )
}}

WITH member_platform_calendar AS (
    SELECT * FROM {{ref('int_calendars__member_platform_calendar')}}
),
member_daily_product_flags AS (
    SELECT * FROM {{ref('int_calendars__member_daily_product_flags')}}
),
joined AS (

    SELECT
      mpc.*,
      max(mpc.date) over (partition by mpc.member_id) >= current_date() as is_member_currently_open,
      dpf.on_demand,
      dpf.primary_coaching,
      dpf.care,
      dpf.coaching_circles,
      dpf.workshops,
      dpf.extended_network,
      dpf.member_daily_product_group
    FROM member_platform_calendar mpc
    LEFT JOIN member_daily_product_flags dpf
        ON mpc.date_key = dpf.date_key AND mpc.member_id = dpf.member_id

)

select * from joined
