--This singular test is needed because it is used to determine the GENERATOR ROW_FUNCTION => (X) 
--in the dim_subscription_billing_cycle. If the max total completed billing cycle ever exceeds the current value set
--in dim_subscription_billing_cycles, specifically in the CTE shown below: 

-- ```
-- all_possible_subscription_cycles as (
--   select
--     billing_dates.*,
--     row_number() over (partition by billing_dates.stripe_subscription_id order by created_at asc) as n,
--     dateadd('month', n * interval_count - 1, created_at) as cycle_start,
--     dateadd('month', (n) * interval_count, created_at) as cycle_end
--   from billing_dates
--   --this means that for every subscription, we will create at most 100 billing cycles from their creation date.
--   --not all of these billing cycles are needed, however, because we will filter down the billing_cycles that are NOT
--   --completed, which are determined if the subscription is ended. A constant value is needed in this generator row_count 
--   --function, but ideally it would be the max value of the completed_billing_cycle for a subscription.
--   cross join table(generator(rowcount => 100))
-- ),
-- ```

--then the billing cycles used to create the date spline will be missing completed billing cycles for subscriptions that exceed the max.
--Therefore, if this tests fails, complete the following steps:

--1. Go to dim_subscription_billing_cycles
--2. Scroll to the all_possible_subscription_cycles and change the line 

-- ``` 
-- select
--     ...
-- cross join table(generator(rowcount => 150)) 
-- ```

--so that the rowcount is greater than the value set in the query.



with util_day as (
    select * from {{ ref('util_day')}}
),

int_customers as (
    select * from {{ ref('int_customers') }}
),

subscriptions as (
    select * from {{ ref('stg_stripe__subscriptions') }}
),

plans as (
    select * from {{ ref('stg_stripe__plans')}}
),

billing_dates as (
  select
    subscriptions.stripe_subscription_id,
    subscriptions.stripe_customer_id,
    int_customers.user_id, --keeping as user_id as a placeholder to change to member_id later if needed
    subscriptions.created_at,
    subscriptions.ended_at,
    plans.interval_count,
    plans.interval,
    floor(datediff('month', subscriptions.created_at, coalesce(subscriptions.ended_at, current_date)) / interval_count) + 1 as total_completed_billing_cycles
  from subscriptions
  left join plans on subscriptions.stripe_plan_id = plans.stripe_plan_id
  left join int_customers
    on subscriptions.stripe_customer_id = int_customers.stripe_customer_id
),

agg as (
    select max(total_completed_billing_cycles) as n from billing_dates
)

select * from agg where n > 100