--import CTEs
with billable_events as (
    select * from {{ ref('fact_billable_events')}}
),

tracks as (
    select * from {{ ref('dim_tracks')}}
),

joined_billable_events as (
    select
        billable_events.*,
        tracks.deployment_type
    from billable_events
    left join tracks
        on billable_events.track_id = tracks.track_id
    --ensure we only grab billable events related to direct pay
    where tracks.deployment_type = 'direct_pay'
),

--KPI Calculation CTE
kpi_calculation as (
    select 
        date_trunc('month', event_at::date) as month,
        'coach_cost' as metric_name,
        {{ dbt_utils.surrogate_key(['metric_name'])}} as metric_name_id,

        --metric calculation
        sum(amount_due_usd) as metric_value
    from joined_billable_events
    group by 1, 2, 3
),

final as (
    select
        {{ dbt_utils.surrogate_key(['month', 'metric_name'])}} as _unique,
        kpi_calculation.*
    from kpi_calculation
)

select * from final