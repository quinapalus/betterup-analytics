with source as (
  select * from {{ref('stg_stripe__products')}}
),

consumer_products as (
  select * from {{ ref('stg_app__consumer_products')}}
),

app_products as (
  select * from {{ ref('stg_app__products')}}
),

joined as (
  select
    source.*,
    --these two columns are used in tandem to ensure that the estimated sessions per month based off of 
    --the product description matches the backend configuration. Actual sessions per month will be used as the SSOT
    --for calculating session utilization metrics. 
    consumer_products.app_consumer_product_id,
    consumer_products.app_product_id,
    consumer_products.estimated_sessions_purchased,
    app_products.sessions_per_month as actual_sessions_per_month
  from source
  left join consumer_products
    on source.stripe_product_Id = consumer_products.stripe_product_id
  left join app_products
    on consumer_products.app_product_id = app_products.product_id
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
from joined
)

select * from final