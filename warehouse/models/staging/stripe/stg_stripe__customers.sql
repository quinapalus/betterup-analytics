with source as (
  select * from {{ source('stripe', 'customers') }}
),

renamed as (
select 
    --primary key
    id as stripe_customer_id,

    --foreign keys
    subscriptions as subscription_id_array,

    --attributes
    email,
    {{ dbt_utils.surrogate_key(['email'])}} as stripe_customer_email_sk,
    name,
    array_size(subscriptions) as count_of_current_subscriptions,
    account_balance,
    balance,
    currency,
    invoice_prefix,
    address as address_object,


    --timestamps
    created::timestamp_ntz as created_at,
    updated::timestamp_ntz as updated_at,


    --misc
    default_source,
    discount,
    delinquent as is_delinquent,
    livemode as is_livemode,
    metadata,
    object,
    preferred_locales
    
from source
)

select * from renamed