with source as (
  select * from {{ source('stripe', 'balance_transactions') }}
),

renamed as (
select 
    --primary key
    id as stripe_balance_transaction_id,

    --foreign keys
    --Contains foreign key to charges, refunds, disputes, etc.
    --can be used to join in charge or refund table.
    source as stripe_source_id,

    --attributes
    type,
    amount/100.00 as amount,
    fee/100.00 as fee,
    fee_details,
    net/100.00 as net,
    status,
    currency,
    description,

    --timestamp
    created::timestamp_ntz as created_at,
    updated::timestamp_ntz as updated_at,

    --misc
    available_on,
    object

from source
)

select * from renamed