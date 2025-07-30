with util_day as (
    select * from {{ ref('util_day')}}
),

int_customers as (
    select * from {{ ref('int_customers') }}
),

subscriptions as (
    select * from {{ ref('int_subscriptions') }}
),

plans as (
    select * from {{ ref('stg_stripe__plans')}}
),

products as (
  select * from {{ ref('int_subscription_products')}}
),

billing_dates as (
  select
    subscriptions.stripe_subscription_id,
    subscriptions.stripe_customer_id,
    subscriptions.stripe_product_id,
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

all_possible_subscription_cycles as (
  select
    billing_dates.*,
    row_number() over (partition by billing_dates.stripe_subscription_id order by created_at asc) as n,
    dateadd('month', n * interval_count - 1, created_at) as cycle_start,
    dateadd('month', (n) * interval_count, created_at) as cycle_end
  from billing_dates
  --this means that for every subscription, we will create at most 100 billing cycles from their creation date.
  --not all of these billing cycles are needed, however, because we will filter down the billing_cycles that are NOT
  --completed, which are determined if the subscription is ended. A constant value is needed in this generator row_count 
  --function, but ideally it would be the max value of the completed_billing_cycle for a subscription.
  cross join table(generator(rowcount => 100))
),

filtered as (
select
--primary key
    {{ dbt_utils.surrogate_key(['stripe_subscription_id', 'cycle_start', 'cycle_end'])}} as _unique,

--foreign keys
    stripe_subscription_id,
    stripe_customer_id,
    stripe_product_id,
    user_id,

--attributes
    n as billing_cycle_order,
    cycle_start,
    cycle_end
from all_possible_subscription_cycles
--ensures we don't grab billing cycles if they haven't completed
where n <= total_completed_billing_cycles
order by stripe_subscription_id, n asc
),

final as (
  select 
    filtered.*,

    --measures
    --exposing directly in the fact table so that we can build a sum 
    --of purchased sessions based on a billing cycle in BI Tool.
    products.actual_sessions_per_month
  from filtered
  left join products
    on filtered.stripe_product_id = products.stripe_product_id
)

select * from final