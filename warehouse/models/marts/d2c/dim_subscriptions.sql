with source as (
    select * from {{ ref('int_subscriptions')}}
),

renamed as (
    select

        --primary key
        stripe_subscription_id,
        
        --foreign keys
        stripe_customer_id,
        stripe_plan_id,
        stripe_product_id,

        --attributes
        /*will help us catch edge cases where subscription has multiple subscription items */
        count_of_subscription_item,
        days_until_due,

        --timestamps
        created_at,
        started_at,
        ended_at,
        canceled_at,
        cancel_at,
        current_period_end_at,
        current_period_start_at,
        trial_end_at,
        trial_start_at,
        updated_at,

        ---useful attributes
        iff(created_at::date = ended_at::date, true, false) as is_ended_same_day_as_created
    from source

)

select * from renamed