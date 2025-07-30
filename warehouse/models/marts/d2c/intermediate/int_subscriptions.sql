with source as (
  select * from {{ref('stg_stripe__subscriptions')}}
),

product_subscription_assignments as (
  select * from {{ ref('int_app__product_subscription_assignments')}}
),

product_subscription_assignment_agg as (
    select 
        stripe_subscription_id,
        --bring the product subscription assignment table to the same granularity of stripe subscriptions.
        array_agg(product_subscription_assignment_id) as arr_product_subscription_assignment_id
    from product_subscription_assignments
    where stripe_subscription_id is not null
    group by 1
),

joined as (
    select
        source.*,
        product_subscription_assignment_agg.arr_product_subscription_assignment_id,
        --this flag determines whether or not a create stripe subscription successfully completes checkout. In stripe
        --subscriptions are created whether or not checkout is completed and a payment if successfully received. If the 
        --payment is void, stripe still creates a stripe_subscription_id. If the payment does not process, a product
        --subscription assignment in the BetterUp backend does not get created. Therefore, this flag helps us filter
        --subscriptions that should not exist in downstream analysis of retention rates and LTV. 
        iff(arr_product_subscription_assignment_id is not null, true, false) as has_created_a_product_subscription_assignment
    from source
    left join product_subscription_assignment_agg
        on source.stripe_subscription_id = product_subscription_assignment_agg.stripe_subscription_id
),

final as (
    select 
        *
    from joined
    where has_created_a_product_subscription_assignment
)

select * from final