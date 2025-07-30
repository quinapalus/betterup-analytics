--import CTEs
with gross_margin as (
    select * from {{ ref('fact_gross_margin_kpi')}}
),

--KPI Calculation CTE
average_forecasted_12_month_revenue_per_subscription as (
    select * from {{ ref('fact_average_forecasted_12_month_revenue_per_subscription_kpi')}}
),

blended_cost_per_subscription as (
    select * from {{ ref('fact_blended_cost_per_subscription_kpi')}}
),

kpi_calculation as (
    select
        average_forecasted_12_month_revenue_per_subscription.month,
        'gross_margin_ltv_to_cac_ratio' as metric_name,

        div0null(average_forecasted_12_month_revenue_per_subscription.metric_value * gross_margin.metric_value, blended_cost_per_subscription.metric_value) as metric_value
    from average_forecasted_12_month_revenue_per_subscription
    left join gross_margin
        on average_forecasted_12_month_revenue_per_subscription.month = gross_margin.month
    left join blended_cost_per_subscription
        on average_forecasted_12_month_revenue_per_subscription.month = blended_cost_per_subscription.month   
),

final as (
    select
        {{ dbt_utils.surrogate_key(['month', 'metric_name'])}} as _unique,
        {{ dbt_utils.surrogate_key(['metric_name'])}} as  metric_name_id,
        kpi_calculation.*
    from kpi_calculation
)

select * from final