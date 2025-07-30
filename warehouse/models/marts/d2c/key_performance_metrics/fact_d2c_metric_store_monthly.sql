--import CTEs
with util_month as (
    select * from {{ ref('util_month')}}
),

metric_targets as (
    select * from {{ ref('stg_gsheets_betterup_metric_targets__metric_targets')}}
),

--KPI Calculation CTE
operational_revenue as (
    select * from {{ ref('fact_operational_revenue_kpi')}}
),

--KPI Calculation CTE
coach_cost as (
    select * from {{ ref('fact_coach_cost_kpi')}}
),

--KPI Calculation CTE
gross_margin as (
    select * from {{ ref('fact_gross_margin_kpi')}}
),

--KPI Calculation CTE
d2c_marketing_spend as (
    select * from {{ ref('fact_digital_marketing_spend_kpi')}}
),

--KPI Calculation CTE
created_subscriptions as (
    select * from {{ ref('fact_created_subscriptions_kpi')}}
),

--KPI Calculation CTE
ended_subscriptions as (
    select * from {{ ref('fact_ended_subscriptions_kpi')}}
),

--KPI Calculation CTE
blended_cost_per_subscription as (
    select * from {{ ref('fact_blended_cost_per_subscription_kpi')}}
),

--KPI Calculation CTE
total_12_month_forecasted_ltv_contribution as (
    select * from {{ ref('fact_total_12_month_forecasted_ltv_contribution_kpi')}}
),

--KPI Calculation CTE
average_forecasted_12_month_revenue_per_subscription as (
    select * from {{ ref('fact_average_forecasted_12_month_revenue_per_subscription_kpi')}}
),

gross_margin_ltv_to_cac_ratio as (
    select * from {{ ref('fact_gross_margin_ltv_to_cac_ratio_kpi')}} 
),

--KPI Calculation CTE
end_of_month_active_subscriptions as (
    select * from {{ ref('fact_end_of_month_active_subscriptions_kpi')}}
),

--KPI Calculation CTE
retention_kpi as (
    select * from {{ ref('fact_retention_kpi')}}
),

--KPI Calculation CTE
session_utilization_kpi as (
    select * from {{ ref('fact_session_utilization_kpi')}}
),

unioned_metric_store as (
    select 
        _unique,
        metric_name_id,
        month,
        metric_name,
        metric_value
    from operational_revenue
    union all 
    select 
        _unique,
        metric_name_id,
        month,
        metric_name,
        metric_value
    from coach_cost
    union all
    select 
        _unique,
        metric_name_id,
        month,
        metric_name,
        metric_value
    from gross_margin
    union all
    select 
        _unique,
        metric_name_id,
        month,
        metric_name,
        metric_value
    from d2c_marketing_spend
    union all
    select 
        _unique,
        metric_name_id,
        month,
        metric_name,
        metric_value
    from created_subscriptions
    union all
    select 
        _unique,
        metric_name_id,
        month,
        metric_name,
        metric_value
    from ended_subscriptions
    union all
    select 
        _unique,
        metric_name_id,
        month,
        metric_name,
        metric_value
    from blended_cost_per_subscription
    union all
    select 
        _unique,
        metric_name_id,
        month,
        metric_name,
        metric_value
    from end_of_month_active_subscriptions
    union all
    select 
        _unique,
        metric_name_id,
        month,
        metric_name,
        metric_value
    from retention_kpi
    union all
    select 
        _unique,
        metric_name_id,
        month,
        metric_name,
        metric_value
    from total_12_month_forecasted_ltv_contribution
    union all
    select 
        _unique,
        metric_name_id,
        month,
        metric_name,
        metric_value
    from average_forecasted_12_month_revenue_per_subscription
    union all
    select
        _unique,
        metric_name_id,
        month,
        metric_name,
        metric_value
    from session_utilization_kpi
    union all
    select
        _unique,
        metric_name_id,
        month,
        metric_name,
        metric_value
    from gross_margin_ltv_to_cac_ratio
),

monthly_metric_targets as (
    select
        *
    from metric_targets
    where period_length = 'month'
        and team = 'direct_to_consumer'
),

final as (
    select 
        --primary key
        unioned_metric_store._unique,

        --foreign key
        unioned_metric_store.metric_name_id,

        --attributes
        unioned_metric_store.month,
        unioned_metric_store.metric_name,

        --Golden Data
        unioned_metric_store.metric_value,
        lag(unioned_metric_store.metric_value, 1) over (partition by unioned_metric_store.metric_name order by unioned_metric_store.month asc) as previous_metric_value,

        monthly_metric_targets.target_metric_value,

        --Useful Target KPI Metrics
        div0null(unioned_metric_store.metric_value, monthly_metric_targets.target_metric_value) as metric_target_value_percent_obtained,
        div0null((unioned_metric_store.metric_value - previous_metric_value), previous_metric_value)as month_over_month_metric_percent_change
    from unioned_metric_store
    left join monthly_metric_targets
        on monthly_metric_targets.metric_name_id = unioned_metric_store.metric_name_id
        and monthly_metric_targets.target_month = unioned_metric_store.month
    where unioned_metric_store.month <= date_trunc('month', current_date)
)

select * from final