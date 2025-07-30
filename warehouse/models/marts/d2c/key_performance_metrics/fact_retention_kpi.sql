--import CTEs
with subscription_daily_status as (
    select * from {{ ref('fact_subscription_daily_status')}} where is_subscription_active
),

created_subscriptions as (
    select * from {{ ref('fact_created_subscriptions_kpi')}}
),

--KPI Calculation CTE
--The following CTEs calculate the business's most important retention touch points at 31, 61, 91 days from creation date of a subscription
retained_subscriptions_at_31_days_from_created as (
    select
        date_trunc('month', date_day) as month,
        'retained_subscriptions_at_31_days_from_created' as metric_name,

        --metric calculation
        count(distinct stripe_subscription_id) as metric_value
    from subscription_daily_status
    where (days_from_created >= 31 and days_from_created < 32)
    group by 1, 2
),

retained_subscriptions_at_61_days_from_created as (
    select
        date_trunc('month', date_day) as month,
        'retained_subscriptions_at_61_days_from_created' as metric_name,

        --metric calculation
        count(distinct stripe_subscription_id) as metric_value
    from subscription_daily_status
    where (days_from_created >= 61 and days_from_created < 62)
    group by 1, 2
),

retained_subscriptions_at_91_days_from_created as (
    select
        date_trunc('month', date_day) as month,
        'retained_subscriptions_at_91_days_from_created' as metric_name,

        --metric calculation
        count(distinct stripe_subscription_id) as metric_value
    from subscription_daily_status
    where (days_from_created >= 91 and days_from_created < 92)
    group by 1, 2
),

retention_rate_at_31_days_from_created as (
    select
        created_subscriptions.month,
        'retention_rate_at_31_days_from_created' as metric_name,

        --metric calculation
        div0null(retained_subscriptions_at_31_days_from_created.metric_value, created_subscriptions.metric_value) as metric_value
    from created_subscriptions
    left join retained_subscriptions_at_31_days_from_created
        on created_subscriptions.month = retained_subscriptions_at_31_days_from_created.month
),

retention_rate_at_61_days_from_created as (
    select
        created_subscriptions.month,
        'retention_rate_at_61_days_from_created' as metric_name,

        --metric calculation
        div0null(retained_subscriptions_at_61_days_from_created.metric_value, created_subscriptions.metric_value) as metric_value
    from created_subscriptions
    left join retained_subscriptions_at_61_days_from_created
        on created_subscriptions.month = retained_subscriptions_at_61_days_from_created.month
),

retention_rate_at_91_days_from_created as (
    select
        created_subscriptions.month,
        'retention_rate_at_91_days_from_created' as metric_name,

        --metric calculation
        div0null(retained_subscriptions_at_91_days_from_created.metric_value, created_subscriptions.metric_value) as metric_value
    from created_subscriptions
    left join retained_subscriptions_at_91_days_from_created
        on created_subscriptions.month = retained_subscriptions_at_91_days_from_created.month
),

kpi_calculation as (
    select * from retained_subscriptions_at_31_days_from_created
    union all
    select * from retained_subscriptions_at_61_days_from_created
    union all
    select * from retained_subscriptions_at_91_days_from_created
    union all
    select * from retention_rate_at_31_days_from_created
    union all
    select * from retention_rate_at_61_days_from_created
    union all
    select * from retention_rate_at_91_days_from_created
),

final as (
    select
        {{ dbt_utils.surrogate_key(['month', 'metric_name'])}} as _unique,
        {{ dbt_utils.surrogate_key(['metric_name'])}} as  metric_name_id,
        kpi_calculation.*
    from kpi_calculation
)

select * from final