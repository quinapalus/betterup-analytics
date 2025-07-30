--import CTEs
with invoice_paid_events as (
    select * from {{ ref('fact_amplitude_invoice_paid_events')}}
),

--KPI Calculation CTE
kpi_calculation as (
    select 
        date_trunc('month', event_time::date) as month,
        'operational_revenue' as metric_name,

        --metric calculation
        sum(amount_paid) as metric_value
    from invoice_paid_events
    where track_deployment_type = 'direct_pay'
    group by 1, 2
),

final as (
    select
        {{ dbt_utils.surrogate_key(['month', 'metric_name'])}} as _unique,
        {{ dbt_utils.surrogate_key(['metric_name'])}} as metric_name_id,
        kpi_calculation.month,
        kpi_calculation.metric_name,
        kpi_calculation.metric_value
    from kpi_calculation
)

select * from final