--KPI Calculation CTE
with created_subscriptions as (
    select * from {{ ref('fact_created_subscriptions_kpi')}}
),

--KPI Calculation CTE
total_12_month_forecasted_ltv_contribution as (
    select * from {{ ref('fact_total_12_month_forecasted_ltv_contribution_kpi')}}
),

--KPI Calculation CTE
kpi_calculation as (
    select
        created_subscriptions.month,
        'average_forecasted_12_month_revenue_per_subscription' as metric_name,

        div0null(total_12_month_forecasted_ltv_contribution.metric_value, created_subscriptions.metric_value) as metric_value
    from created_subscriptions
    left join total_12_month_forecasted_ltv_contribution
        on created_subscriptions.month = total_12_month_forecasted_ltv_contribution.month
),

final as (
    select
        {{ dbt_utils.surrogate_key(['month', 'metric_name'])}} as _unique,
        {{ dbt_utils.surrogate_key(['metric_name'])}} as  metric_name_id,
        kpi_calculation.*
    from kpi_calculation
)

select * from final