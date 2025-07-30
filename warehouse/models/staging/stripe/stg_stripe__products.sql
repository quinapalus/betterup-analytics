with source as (
  select * from {{ source('stripe', 'products') }}
),

renamed as (
select 
    --primary key
    id as stripe_product_id,

    --attributes
    name,
    active as is_active,
    description,
    type,    

    --timestamps
    created::timestamp_ntz as created_at,
    updated::timestamp_ntz as updated_at,

    --misc
    livemode as is_livemode,
    metadata,
    attributes,
    images,
    object

from source
)

select * from renamed