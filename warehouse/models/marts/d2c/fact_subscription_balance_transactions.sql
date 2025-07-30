{{ config
    (
        tags=['finance', 'revenue']
    )
}}

with stripe_balance_transactions as (
    select * from {{ ref('stg_stripe__balance_transactions')}}
),

stripe_charges as (
    select * from {{ref('stg_stripe__charges')}}
),

stripe_invoices as (
    select * from {{ref('stg_stripe__invoices')}}
),

stripe_subscriptions as (
    select * from {{ ref('int_subscriptions')}}
),

stripe_plans as (
    select * from {{ ref('stg_stripe__plans')}}
),

stripe_products as (
    select * from {{ ref('stg_stripe__products') }}
),

product_subscription_assignments as (
    select * from {{ ref('int_app__product_subscription_assignments')}}
),

int_users as (
    select * from {{ ref('int_app__users')}}
),

consumer_gift_cards as (
    select * from {{ ref('stg_app__consumer_gift_cards')}}
),

agg_product_subscription_assignments as (
    /*Provides mapping between member id and stripe customer id
    */
    select 
        distinct
        member_id,
        stripe_customer_id
    from product_subscription_assignments
),

joined as (
    select
        stripe_balance_transactions.*,

        --bring in desired stripe charge columns
        --booleans
        stripe_charges.is_paid,
        stripe_charges.is_captured,
        stripe_charges.is_refunded,
        stripe_charges.is_disputed,
        stripe_charges.stripe_customer_id,
        stripe_charges.stripe_payment_intent_id,

        ---charge billing details
        stripe_charges.billing_city,
        stripe_charges.billing_state,
        stripe_charges.billing_country,
        stripe_charges.billing_address_line_1,
        stripe_charges.billing_address_line_2,
        stripe_charges.billing_postal_code,  

        --bring in desired stripe_invoice columns
        --TODO: Refactor with dim_invoices and update Looker
        stripe_invoices.stripe_invoice_id,
        stripe_invoices.stripe_subscription_id,
        stripe_invoices.stripe_coupon_id,
        
        --bring in subscription attributes
        --TODO: Refactor with dim_subscriptions and update Looker
        stripe_subscriptions.stripe_plan_id,
        stripe_subscriptions.stripe_product_id,

        --product subscription user_id mapping
        agg_product_subscription_assignments.member_id,

        --users
        int_users.user_id,

        --gift card foreign keys to allow downstream joins & improved usability
        consumer_gift_cards.app_consumer_gift_card_id
    from stripe_balance_transactions
    left join stripe_charges  
        on stripe_balance_transactions.stripe_balance_transaction_id = stripe_charges.stripe_balance_transaction_id
    left join stripe_invoices 
        on stripe_charges.stripe_invoice_id = stripe_invoices.stripe_invoice_id
    left join int_users
        on stripe_charges.stripe_customer_email_sk = int_users.app_user_email_sk
    left join stripe_subscriptions
        on stripe_invoices.stripe_subscription_id = stripe_subscriptions.stripe_subscription_id
    left join stripe_plans
        on stripe_subscriptions.stripe_plan_id = stripe_plans.stripe_plan_id
    left join stripe_products
        on stripe_subscriptions.stripe_product_id = stripe_products.stripe_product_id
    left join agg_product_subscription_assignments
        on stripe_charges.stripe_customer_id = agg_product_subscription_assignments.stripe_customer_id
    left join consumer_gift_cards 
        on stripe_charges.stripe_payment_intent_id = consumer_gift_cards.stripe_payment_intent_id
),

final as (
    select 
    --primary key
        stripe_balance_transaction_id,

    --foreign keys
        stripe_source_id,
        stripe_customer_id,
        stripe_invoice_id,
        stripe_subscription_id,
        stripe_plan_id,
        stripe_product_id,
        stripe_payment_intent_id,
        stripe_coupon_id,
        member_id,
        user_id,
        app_consumer_gift_card_id,

    --attributes
        type,
        amount,
        fee,
        net,

        --balance transaction details
        is_paid,
        is_captured,
        is_refunded,
        is_disputed,
        status,
        currency,

        --billing details
        billing_city,
        billing_state,
        billing_country,
        billing_address_line_1,
        billing_address_line_2,
        billing_postal_code,

    --timestamps
        created_at,
        updated_at
from joined
)

select * from final
