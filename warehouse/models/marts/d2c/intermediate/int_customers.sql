with stripe_customers as (
    select * from {{ ref('stg_stripe__customers')}}
),

int_users as (
    select * from {{ ref('int_app__users')}}
),

product_subscription_assignments as (
  select * from {{ ref('int_app__product_subscription_assignments')}}
),

app_user_to_stripe_mapping as (
    select 
        distinct
            member_id as user_id,
            stripe_customer_id
    from product_subscription_assignments
),

joined as (
    select
        stripe_customers.*,
        coalesce(app_user_to_stripe_mapping.user_id, int_users.user_id) as user_id
    from stripe_customers
    left join app_user_to_stripe_mapping
        on stripe_customers.stripe_customer_id = app_user_to_stripe_mapping.stripe_customer_id
    left join int_users
        on stripe_customers.stripe_customer_email_sk = int_users.app_user_email_sk
)
 
select * from joined