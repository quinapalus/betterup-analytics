{{
  config(
    tags=['eu']
  )
}}

with member_platform_calendar as (

  select * from {{ ref('int_calendars__member_platform_calendar') }}

),

product_agg as (

  select
    member_id,
    date,
    date_key,
    boolor_agg(psa_includes_on_demand) as on_demand,
    boolor_agg(psa_includes_primary_coaching) as primary_coaching,
    boolor_agg(psa_includes_care) as care,
    boolor_agg(psa_includes_coaching_circles) as coaching_circles,
    boolor_agg(psa_includes_workshops) as workshops,
    boolor_agg(psa_includes_extended_network) as extended_network
  from member_platform_calendar
  group by
    member_id,
    date,
    date_key

),

final as (
  select
    {{ dbt_utils.surrogate_key(['member_id', 'date_key']) }} as member_daily_product_flags_id,
    *,
    {{ sanitize_product_group('on_demand','primary_coaching','care','coaching_circles','workshops','extended_network') }} AS member_daily_product_group
  from product_agg
)

select *
from final
