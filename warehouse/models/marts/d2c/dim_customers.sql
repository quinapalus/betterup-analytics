with int_customers as (
    select * from {{ ref('int_customers')}}
),

final as (
    select
        --primary
        stripe_customer_id,

        --foreign key
        user_id,

        --attributes
        created_at,
        count_of_current_subscriptions
    from int_customers
)

select * from final