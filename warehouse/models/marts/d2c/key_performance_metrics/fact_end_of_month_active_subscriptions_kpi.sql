with subscription_daily_status as (
    select * from {{ ref('fact_subscription_daily_status')}} 
),

filtered_active_subs as (
    select 
        *
    from subscription_daily_status
    where is_subscription_active
        --grab only last day of the month to report on active subs
        and is_last_date_day_of_month
),

--KPI Calculation CTE
kpi_calculation as (
    select 
        date_trunc('month', date_day) as month,
        'end_of_month_active_subscriptions' as metric_name,
       {{ dbt_utils.surrogate_key(['metric_name'])}} as metric_name_id,

        --metric calculation
        count(distinct stripe_subscription_id) as metric_value
    from filtered_active_subs
    group by 1, 2, 3
),

final as (
    select
        {{ dbt_utils.surrogate_key(['month', 'metric_name'])}} as _unique,
        kpi_calculation.*
    from kpi_calculation
)

select * from final
