with source as (
  select * from {{ source('stripe', 'plans') }}
),

renamed as (
select 
    --primary key
    id as stripe_plan_id,

    --foreign keys
    product as stripe_product_id,

    --attributes
    name as plan_name,
    nickname,

    --plan billing & amount details
    amount/100.00 as amount,
    amount_decimal,
    billing_scheme,
    interval,
    interval_count,

    --plan details
    active as is_active,
    currency,
    statement_descriptor,
    usage_type,


    --timestamps
    created::timestamp_ntz as created_at,
    updated::timestamp_ntz as updated_at,


    --misc
    livemode as is_livemode,
    metadata,
    object

from source
)

select * from renamed