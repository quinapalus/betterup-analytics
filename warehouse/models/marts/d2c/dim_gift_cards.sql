with source as (
    select * from {{ ref('stg_app__consumer_gift_cards')}}
),

final as (
select
    --primary key
    app_consumer_gift_card_id,

    --foreign key
    stripe_payment_intent_id,
    user_id,
    stripe_promo_code_id,
    consumer_coupon_id,

    --attributes
    giftee_email,    
    gifter_email,
    has_showed_gift_reminder,   

    ----timestamps
    created_at,
    updated_at,
    voided_at,
    paid_at,
    redeemed_at
from source
)

select * from final