
with product_subscriptions as (
  select * from {{ ref('int_app__product_subscriptions') }}
)

select
    --Primary Key
    product_subscription_id,

    --Foreign Keys
    product_id,
    organization_id,
    subscription_terms_id,

    --Logical data
    state,
    name,
    care_limit,
    care_limit_cadence,
    coaching_circles_limit,
    coaching_circles_limit_cadence,
    exact_specialist_verticals,
    non_transfer_period,
    on_demand_limit,
    on_demand_limit_cadence,
    primary_coaching_limit,
    primary_coaching_limit_cadence,
    specialist_coaching_limit,
    specialist_coaching_limit_cadence,
    transferable,

    --Timestamps
    created_at,
    updated_at
    
from product_subscriptions 