--import CTEs
with subscription_billing_cycles as (
    select * from {{ ref('fact_subscription_billing_cycles')}}
),

billable_events as (
    select * from {{ ref('fact_billable_events')}}
),

agg_purchased_sessions as (
select 
    date_trunc('month', cycle_end::date) as month,

    sum(actual_sessions_per_month) as total_purchased_sessions
from subscription_billing_cycles
group by 1
),

agg_valid_billable_events as (
    select
        date_trunc('month', subscription_billing_cycles.cycle_end::date) as month,
        count(distinct billable_event_id) as total_billable_events
    from subscription_billing_cycles
    left join billable_events
        on subscription_billing_cycles.stripe_subscription_id = billable_events.stripe_subscription_id
        and subscription_billing_cycles.user_id = billable_events.member_id
        and billable_events.event_at >= subscription_billing_cycles.cycle_start
        and billable_events.event_at < subscription_billing_cycles.cycle_end
    group by 1
),

joined as (
    select 
        agg_purchased_sessions.*,
        agg_valid_billable_events.total_billable_events,
        'session_utilization' as metric_name,
        div0null(agg_valid_billable_events.total_billable_events, agg_purchased_sessions.total_purchased_sessions) as metric_value
    from agg_purchased_sessions
    left join agg_valid_billable_events
        on agg_purchased_sessions.month = agg_valid_billable_events.month
),

kpi_calculation as (
    select
    ----primary key
        {{ dbt_utils.surrogate_key(['month', 'metric_name'])}} as _unique,
        {{ dbt_utils.surrogate_key(['metric_name'])}} as metric_name_id,

        month,
        metric_name,
        metric_value
    from joined
)

select * from kpi_calculation