--import CTEs
with operational_revenue as (
    select * from {{ ref('fact_operational_revenue_kpi')}}
),

--KPI Calculation CTE
coach_cost as (
    select * from {{ ref('fact_coach_cost_kpi')}}
),

--KPI Calculation CTE
kpi_calculation as (
    select 
        operational_revenue.month,
        'gross_margin' as metric_name,

        --metric calculation
        div0null((operational_revenue.metric_value - coach_cost.metric_value), operational_revenue.metric_value) as  metric_value
    from operational_revenue
    left join coach_cost
        on operational_revenue.month = coach_cost.month
),

final as (
    select
        {{ dbt_utils.surrogate_key(['month', 'metric_name'])}} as _unique,
        --parse out dbt_utils generate surrogate key function to avoid
        --ambigous column reference
        {{ dbt_utils.surrogate_key(['metric_name'])}} as  metric_name_id,

        kpi_calculation.*
    from kpi_calculation
)

select * from final