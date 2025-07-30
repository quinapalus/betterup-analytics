
with forecasted_retention_table as (
    select * from {{ ref('dim_plan_forecasted_retention_rates_by_month') }}
),

subscription_daily_status as (
    select * from {{ ref('fact_subscription_daily_status') }}
    where is_subscription_active
),

subscriptions as (
    select * from {{ ref('dim_subscriptions') }}
),

customers as (
    select * from {{ ref('dim_customers') }}
),

plans as (
    select * from {{ ref('dim_plans')}}
),

members as (
    select * from {{ ref('dim_members') }}
),

joined as (
select
    subscription_daily_status.*,
    subscriptions.created_at,
    plans.stripe_plan_id,
    plans.monthly_amount,
    plans.plan_interval_billing_structure,
    plans.amount,
    members.member_id
from subscription_daily_status
left join subscriptions
    on subscription_daily_status.stripe_subscription_id = subscriptions.stripe_subscription_id
left join plans
    on subscriptions.stripe_plan_id = plans.stripe_plan_id
left join customers
    on subscriptions.stripe_customer_id = customers.stripe_customer_id
left join members
    on customers.user_id = members.member_id
),

cohort_size as (
    select
        date_trunc('month', created_at::date) as subscription_created_month,
        stripe_plan_id,
        monthly_amount,

        --aggregations
        count(distinct member_id) as total_unique_member_in_cohort
    from joined
    --need monthly amount as a separate grouped column to build Looker measure for LTV contribution (= forecasted_members * monthly_amount)
    group by 1, 2, 3
),

final as (
    select
        --primary key
        {{dbt_utils.surrogate_key(['cohort_size.subscription_created_month',
                                'cohort_size.stripe_plan_id',
                                'forecasted_retention_table.months_from_created'])}} as _id,

        --cohort data
        cohort_size.subscription_created_month,
        cohort_size.stripe_plan_id,
        cohort_size.monthly_amount,
        cohort_size.total_unique_member_in_cohort,

        --forecasted values
        forecasted_retention_table.months_from_created,
        forecasted_retention_table.forecasted_retention_rate,
        cohort_size.total_unique_member_in_cohort * forecasted_retention_rate as forecasted_retained_members,

        --measures
        forecasted_retained_members * monthly_amount as plan_month_ltv_contribution
    from cohort_size
    left join forecasted_retention_table
        on cohort_size.stripe_plan_id = forecasted_retention_table.stripe_plan_id
)

select * from final