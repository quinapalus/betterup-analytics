with source as (
    select * from {{ source('app', 'consumer_gift_cards') }}
),

renamed as (
    select 
        --primary key
        id as app_consumer_gift_card_id,

        --foreign keys
        user_id,
        stripe_payment_id as stripe_payment_intent_id,
        stripe_promo_code_id,
        consumer_coupon_id,

        --attributes
        giftee_email,
        giftee_full_name,
        gifter_email,
        gifter_full_name,
        showed_gift_reminder as has_showed_gift_reminder,
        
        ----timestamps
        updated_at,
        created_at,
        voided_at,
        paid_at,
        redeemed_at
        
    from source
)

select * from renamed