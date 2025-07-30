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
    select * from {{ ref('dim_plans') }}
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

plan_months_from_created_member_agg as (
--using the number of months away from the creation date of all active subscriptions, we count the number of unique 
--members, for a given plan, that are still active.
    select
        stripe_plan_id,
        months_from_created,

        --aggregations
        count(distinct member_id) as total_unique_member
    from joined
    group by 1, 2
    order by 1, 2 desc
),

plan_initial_cohort as (
--To build a forecasted retention rate table, we then grab the initial cohort size for all stripe plans using 
--months from the creation date = 0 as the initial cohort size.
    select
        stripe_plan_id,
        months_from_created,

        --aggregations
        count(distinct member_id) as total_unique_member
    from joined
    where months_from_created = 0
    group by 1, 2
    order by 1, 2 desc
),

plan_retention_table as (
select

--primary key
    {{dbt_utils.surrogate_key(['plan_months_from_created_member_agg.stripe_plan_id',
                                'plan_months_from_created_member_agg.months_from_created'
                                ])}} as _id,

    plan_months_from_created_member_agg.stripe_plan_id,
    plan_months_from_created_member_agg.months_from_created,
    plan_initial_cohort.total_unique_member as total_unique_member_first_month,

    --calculate forecasted retention rate
    div0null(plan_months_from_created_member_agg.total_unique_member, total_unique_member_first_month) as forecasted_retention_rate
from plan_months_from_created_member_agg
left join plan_initial_cohort
    on plan_months_from_created_member_agg.stripe_plan_id = plan_initial_cohort.stripe_plan_id
)

select * from plan_retention_table