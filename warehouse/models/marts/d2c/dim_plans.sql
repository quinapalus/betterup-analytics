with source as (
    select * from {{ ref('stg_stripe__plans')}}
),

final as (
    select
    --primary key
        stripe_plan_id,

    --foreign keys
        stripe_product_id,

    --attributes
        plan_name,

    --plan billing & amount details
        amount,
        billing_scheme,
        interval,
        interval_count,
        interval_count::string || '-' || interval as plan_interval_billing_structure,
        case
            when interval = 'month'
            --interval count can be = 1 or = 6
                then amount/interval_count
            when interval = 'year'
                then amount/(12 * interval_count)
        end as monthly_amount,

    --plan details
        is_active,
        currency

    from source
)

select * from final