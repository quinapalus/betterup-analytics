with util_day as (
    select * from {{ ref('util_day')}}
),

util_month as (
    select * from {{ ref('util_month')}}
),

subscriptions as (
    select * from {{ ref('int_subscriptions')}}
),

joined as (
    select
        util_day.date_day,
        subscriptions.stripe_subscription_id,
        subscriptions.stripe_customer_id,
        subscriptions.created_at::date as subscription_created_date,
        subscriptions.ended_at::date as subscription_ended_date,
        subscriptions.canceled_at::date as subscription_canceled_date,

        dateadd('month', 12, subscriptions.created_at::date) as created_month_plus_12,
        date_trunc('month', subscriptions.created_at::date) as subscription_created_month
    from util_day
    cross join subscriptions
    where true
    
    -- calendar date greater than or equal to start date to fan out records for all future dates
    and util_day.date_day >= date_trunc('day', subscriptions.created_at)
    --and util_day.date_day < date_trunc('day', subscriptions.ended_at)
    -- ... within reason (12 months out)
    --AND util_day.date_month < created_month_plus_12
    -- And requiring there is a paid subscription that was not canceled the same day.
    -- AND created_at IS NOT NULL
    -- AND IFF(subscriptions.canceled_at IS NOT NULL,
    --       subscriptions.created_at::date != subscriptions.canceled_at::date,
    --       1=1)

    -- Legacy code in Looker
    -- calendar date greater than or equal to start date to fan out records for all future dates
    -- ${calendar_v1_cancel.calendar_date} > ${consumer_subscriptions.paid_subscription_start_date}
    -- -- ... within reason (12 months out)
    -- AND ${calendar_v1_cancel.year_month} < ${consumer_subscriptions.start_month_plus_12}
    -- -- And requiring there is a paid subscription that was not canceled the same day.
    -- AND ${consumer_subscriptions.paid_subscription_start_date} IS NOT NULL
    -- AND IFF(${pricing_plan_subscriptions.CANCELED_AT_raw} IS NOT NULL,
    --       ${consumer_subscriptions.paid_subscription_start_date} != ${pricing_plan_subscriptions.CANCELED_AT_date},
    --       1=1)
),

final as (
    select
        ---primary key
        {{ dbt_utils.surrogate_key(['date_day', 'stripe_subscription_id'])}} as _unique,

        --attributes
        date_day,
        last_day(date_day, 'month') as last_date_day_of_month,
        iff(date_day = last_day(date_day, 'month'), true, false) as is_last_date_day_of_month,
        iff(date_day = date_trunc('month', date_day), true, false) as is_first_date_day_of_month,
        stripe_subscription_id,
        stripe_customer_id,

        --attributes
        case 
            --this condition checks if both the ended date and subscription cancel date exist. If they do, grab the timestamp
            --that is earlier.
            when (subscription_ended_date is not null and subscription_canceled_date is not null) and subscription_ended_date >= subscription_canceled_date
                then subscription_canceled_date
            when (subscription_ended_date is not null and subscription_canceled_date is not null) and subscription_ended_date < subscription_canceled_date
                then subscription_ended_date
            --this condition checks if the subscription does not have a cancel_date, but has an ended_date. Then, grab the ended date.
            when (subscription_ended_date is not null and subscription_canceled_date is null) 
                then subscription_ended_date
            --this condition checks if the ended date is null and the canceled_date is not null, then grab the canceled_date.
            when (subscription_ended_date is null and subscription_canceled_date is not null) 
                then subscription_canceled_date
        end as subscription_retention_ended_at,

        case
            --if neither the ended at date or cancel date is populated, a subscription is active
            when subscription_retention_ended_at is null
                then true
            when date_day >= subscription_retention_ended_at
                then false
            else true
        end as is_subscription_retention_active,

        case
            --if neither the ended at date or cancel date is populated, a subscription is active
            when subscription_ended_date is null
                then true
            when date_day >= subscription_ended_date
                then false
            else true
        end as is_subscription_active,

        subscription_created_month,

        subscription_created_date,
        subscription_ended_date,
        subscription_canceled_date,

        --cohort values
        datediff('day', subscription_created_date, date_day) as days_from_created,
        datediff('week', subscription_created_date, date_day) as weeks_from_created,
        datediff('month', subscription_created_date, date_day) as months_from_created

    from joined
)

select * from final