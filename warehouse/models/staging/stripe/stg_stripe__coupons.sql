with source as (
  select * from {{ source('stripe', 'coupons') }}
),

renamed as (
select 
    --primary key
    id as stripe_coupon_id,

    --foreign keys

    --attributes
    amount_off/100.00 as amount_off,
    currency,
    duration,
    livemode as is_livemode,
    times_redeemed,
    max_redemptions,
    metadata,
    name,
    object,
    percent_off_precise,
    percent_off,
    duration_in_months,
    

    --timestamps
    created::timestamp as created_at,
    updated::timestamp as updated_at,
    redeem_by
    
from source
)

select * from renamed