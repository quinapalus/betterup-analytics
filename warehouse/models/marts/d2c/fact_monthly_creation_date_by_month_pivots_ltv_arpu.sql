with subscription_daily_status as (
    select * from {{ ref('fact_subscription_daily_status')}}
    where is_subscription_active
),

subscriptions as (
    select * from {{ ref('dim_subscriptions')}}
),

customers as (
    select * from {{ ref('dim_customers')}}
),

plans as (
    select * from {{ ref('dim_plans')}}
),

members as (
    select * from {{ ref('dim_members')}}
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

        --aggregations
        count(distinct member_id) as total_unique_member,
        count(distinct stripe_customer_id) as total_unique_customer,
        count(distinct stripe_subscription_id) as total_unique_subscription
    from joined
    group by 1, 2
),

agg as (
    select
        date_trunc('month', created_at::date) as subscription_created_month,
        months_from_created,
        stripe_plan_id,

        --aggregations
        count(distinct member_id) as total_unique_member,
        count(distinct stripe_customer_id) as total_unique_customer,
        count(distinct stripe_subscription_id) as total_unique_subscription
    from joined
    group by 1, 2, 3
),

report as (
select 
    agg.*,
    --useful columns to tag a reporting period as completed or not, meaning that the numbers in that record will not further update. 
    dateadd('month', agg.months_from_created, agg.subscription_created_month) as pivot_reporting_month,
    current_date,
    last_day(pivot_reporting_month) as last_day_of_pivot_reporting_month,
    iff(last_day_of_pivot_reporting_month < current_date, true, false) is_complete_reporting_pivot,

    cohort_size.total_unique_member as cohort_total_unique_member
from agg
left join cohort_size
    on agg.subscription_created_month = cohort_size.subscription_created_month
    and agg.stripe_plan_id = cohort_size.stripe_plan_id
),

retention_table as (
    --used to get normalized retention rates by stripe_plan_id with 
    --months from the creation date as a variable.
select 
    stripe_plan_id,
    months_from_created,

    sum(total_unique_member) as total_retained,
    sum(cohort_total_unique_member) as total_cohort_size,
    div0null(total_retained, total_cohort_size) as retention_rate
from report
--ensures that we only calculate these normalized retention rates using completed periods.
where is_complete_reporting_pivot
group by 1, 2
),

ltv_pivot_contribution_calculation as (
select
    --primary key
    {{dbt_utils.surrogate_key(['report.subscription_created_month',
                                'report.stripe_plan_id',
                                'report.months_from_created'])}} as _id,

    --foreign keys
    report.stripe_plan_id,

    --attributes
    report.subscription_created_month,
    report.months_from_created,
    report.pivot_reporting_month,
    report.is_complete_reporting_pivot,
    plans.monthly_amount,

    --measures    
    retention_table.total_retained,
    retention_table.total_cohort_size,
    retention_table.retention_rate,
    cohort_total_unique_member * monthly_amount * retention_rate as forecasted_ltv_contribution

from report
left join retention_table
    on report.months_from_created = retention_table.months_from_created
    and report.stripe_plan_id = retention_table.stripe_plan_id
left join plans
    on report.stripe_plan_id = plans.stripe_plan_id
)

select * from ltv_pivot_contribution_calculation