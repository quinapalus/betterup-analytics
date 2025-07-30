with invoice_items as (
    select * from {{ ref('stg_stripe__invoice_items')}}
),

invoices as (
    select * from {{ ref('stg_stripe__invoices')}}
),

charges as (
    select * from {{ ref('stg_stripe__charges')}}
),

int_user as (
    select * from {{ ref('int_app__users')}}
),

consumer_gift_cards as (
    select * from {{ ref('stg_app__consumer_gift_cards')}}
),

invoice_item_joined as (
    select
        invoice_items.*,
        --invoice attributes
        invoices.stripe_coupon_id,
        invoices.stripe_subscription_id,
        invoices.stripe_charge_id
    from invoice_items
    left join invoices 
        on invoice_items.stripe_invoice_id = invoices.stripe_invoice_id
),

charge_invoice_item_join as (
    select 

        --foreign keys
        int_user.user_id,
        charges.stripe_charge_id,
        charges.stripe_balance_transaction_id,
        charges.stripe_payment_intent_id,
        charges.stripe_card_id,
        charges.stripe_customer_id,
        charges.stripe_customer_email_sk,
        invoice_item_joined.stripe_invoice_item_id,
        invoice_item_joined.stripe_invoice_id,
        invoice_item_joined.stripe_product_id,
        invoice_item_joined.stripe_plan_id,
        invoice_item_joined.stripe_coupon_id,
        
        --attributes
        --we do not want charge amount as the sum of that value will be incorrect.
        charges.created_at, 
        invoice_item_joined.amount,
        charges.is_captured,
        charges.is_paid,
        charges.status,

        --billing details
        billing_city,
        billing_state,
        billing_country,
        billing_address_line_1,
        billing_address_line_2,
        billing_postal_code,

        --consumer gift card foreign key to enable downstream joins in Looker
        consumer_gift_cards.app_consumer_gift_card_id
    from charges
    --for invoices with multiple invoice_items, this join will 
    --create a secondary record for the charge. This is intentional
    --as we want to track all charges and the invoice_line_item amount captured. 
    left join invoice_item_joined
        on charges.stripe_charge_id = invoice_item_joined.stripe_charge_id
    left join int_user
        on charges.stripe_customer_email_sk = int_user.app_user_email_sk
    left join consumer_gift_cards
        on charges.stripe_payment_intent_id = consumer_gift_cards.stripe_payment_intent_id
),

final as (
    select
        --primary key
        {{ dbt_utils.surrogate_key(['stripe_charge_id', 'stripe_invoice_item_id'])}} as invoice_item_charge_id,
        *
    from charge_invoice_item_join
)

select * from final