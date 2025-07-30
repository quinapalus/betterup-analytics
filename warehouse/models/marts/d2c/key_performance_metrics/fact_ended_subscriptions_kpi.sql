--import CTEs
with subscriptions as (
    select * from {{ ref('dim_subscriptions')}}
),

--KPI Calculation CTE
kpi_calculation as (
    select 
        date_trunc('month', ended_at::date) as month,
        'ended_subscriptions' as metric_name,
       {{ dbt_utils.surrogate_key(['metric_name'])}} as metric_name_id,

       --metric calculation
        count(distinct stripe_subscription_id) as metric_value
    from subscriptions
    where ended_at is not null
    group by 1, 2, 3
),

final as (
    select
        {{ dbt_utils.surrogate_key(['month', 'metric_name'])}} as _unique,
        kpi_calculation.*
    from kpi_calculation
)

select * from final