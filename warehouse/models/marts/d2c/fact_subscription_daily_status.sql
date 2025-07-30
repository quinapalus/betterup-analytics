with daily_status as (
    select * from {{ ref('int_stripe_subscription_daily_status')}}
    --where is_subscription_active
),

final as (
    select
        ---primary key
        _unique,

        --attributes
        date_day,
        stripe_subscription_id,
        stripe_customer_id,

        --attributes
        is_subscription_active,
        is_subscription_retention_active, 
        is_last_date_day_of_month,
        is_first_date_day_of_month,
        
        --cohort values
        days_from_created,
        weeks_from_created,
        months_from_created
    from daily_status
)

select * from final 