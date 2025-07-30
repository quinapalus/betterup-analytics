with coupon as (
    select * from {{ref('stg_stripe__coupons')}}
),

final as (
    select
    --primary key
        stripe_coupon_id,

        --foreign keys

        --attributes
        name,
        amount_off,
        currency,
        duration,
        times_redeemed,
        max_redemptions,
        created_at
    from coupon
)

select * from final