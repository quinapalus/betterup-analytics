with source as (
  select * from {{ ref('int_subscription_products') }}
),

final as (
select 
    --primary key
    stripe_product_id,

    --foreign keys (will be useful for debugging).
    app_consumer_product_id,
    app_product_id,

    --attributes
    name,
    is_active,
    type,
    estimated_sessions_purchased,
    actual_sessions_per_month,    

    --timestamps
    created_at,
    updated_at
from source
)

select * from final