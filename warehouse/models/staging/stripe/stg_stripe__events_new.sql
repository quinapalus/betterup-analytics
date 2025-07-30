with source as (
  select * from {{ source('stripe', 'events') }}
),

renamed as (
select 
    --primary key
    id as stripe_event_id,


    --attributes
    data,
    parse_json(data)['object'] as data_object,
    type as event_type,

    --timestamps
    created::timestamp_ntz as created_at,
    updated::timestamp_ntz as updated_at,
    
    --misc
    api_version,
    request,
    livemode as is_livemode,
    pending_webhooks
    
from source
)

select * from renamed