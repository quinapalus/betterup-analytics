with source as (
  select * from {{ source('stripe', 'invoice_items') }}
),

renamed as (
select 
    --primary key
    id as stripe_invoice_item_id,

    --foreign keys
    customer as stripe_customer_id,
    invoice as stripe_invoice_id,
    plan['id']::string as stripe_plan_id,
    plan['product']::string as stripe_product_id,

    --attributes
    amount,
    object,
    description,
    discountable as is_discountable,
    discounts,
    livemode as is_livemode,
    metadata,

    --plan attributes
    plan as plan_object,
    plan['active']::boolean as is_active,

    --timestamps
    date,
    period['end']::timestamp as ended_at,
    period['start']::timestamp as started_at
    
from source
)

select * from renamed