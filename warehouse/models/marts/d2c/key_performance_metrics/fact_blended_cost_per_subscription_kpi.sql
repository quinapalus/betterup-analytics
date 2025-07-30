--import CTEs
with d2c_marketing_spend as (
    select * from {{ ref('fact_digital_marketing_spend_kpi')}}
),

--KPI Calculation CTE
created_subscriptions as (
    select * from {{ ref('fact_created_subscriptions_kpi')}}
),

--KPI Calculation CTE
kpi_calculation as (
    select
        created_subscriptions.month,
        'blended_cost_per_subscription' as metric_name,
    
        --metric calculation (depends on previous 2 calculated metrics)
        div0null(d2c_marketing_spend.metric_value, created_subscriptions.metric_value) as metric_value
    from created_subscriptions
    left join d2c_marketing_spend
        on created_subscriptions.month = d2c_marketing_spend.month
),

final as (
    select
        {{ dbt_utils.surrogate_key(['month', 'metric_name'])}} as _unique,
        {{ dbt_utils.surrogate_key(['metric_name'])}} as  metric_name_id,
        kpi_calculation.*
    from kpi_calculation
)

select * from final