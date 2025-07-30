with forecasted_ltv as (
    select * from {{ ref('fact_monthly_creation_date_by_month_pivots_forecasted_ltv')}}
),

--KPI Calculation CTE
kpi_calculation as (
    select
        subscription_created_month as month,
        'total_12_month_forecasted_ltv_contribution' as metric_name,
       {{ dbt_utils.surrogate_key(['metric_name'])}} as metric_name_id,

        sum(plan_month_ltv_contribution) as metric_value
    from forecasted_ltv
    where months_from_created <= 12
    group by 1, 2, 3
),

final as (
    select
        {{ dbt_utils.surrogate_key(['month', 'metric_name'])}} as _unique,
        kpi_calculation.*
    from kpi_calculation
)

select * from final