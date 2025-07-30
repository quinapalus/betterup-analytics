with source as (
  select * from {{ ref('stg_stripe__invoices')}}
),

renamed as (
select 
    --primary key
    stripe_invoice_id,

    --foreign keys
    stripe_charge_id,
    stripe_customer_id,
    stripe_payment_intent_id,
    stripe_subscription_id,
    stripe_coupon_id,

    --attributes
    amount_due,
    amount_paid,
    amount_remaining,
    is_paid,   

    --timestamp
    created_at,
    updated_at,
    finalized_at,
    period_end_at,
    period_start_at
from source
)

select * from renamed