with source as (
  select * from {{ source('stripe', 'charges') }}
),

renamed as (
select 
    --primary key
    id as stripe_charge_id,

    --foreign keys
    application as stripe_application_id,
    balance_transaction as stripe_balance_transaction_id,
    invoice as stripe_invoice_id,
    card['id']::string as stripe_card_id,
    customer as stripe_customer_id,
    payment_intent as stripe_payment_intent_id,
    payment_method as stripe_payment_method,

    --attributes
    amount/100.00 as amount,
    amount_captured/100.00 as amount_captured,
    amount_refunded/100.00 as amount_refunded,

    --payment details
    calculated_statement_descriptor,
    payment_method_details,
    payment_method,
    paid as is_paid,
    captured as is_captured,
    refunded as is_refunded,
    receipt_email,
    receipt_url,
    receipt_number,
    description,
    status,
    card as card_json_object,
    disputed as is_disputed,

    --failure/fraud
    failure_code,
    failure_message,
    fraud_details,

    --timestamp
    created::timestamp_ntz as created_at,
    updated::timestamp_ntz as updated_at,

    --misc
    livemode as is_livemode,
    metadata,
    object,
    outcome,

    --billing data
    billing_details,
    parse_json(billing_details)['address'] as address_object,
    address_object['city']::string as billing_city,
    address_object['state']::string as billing_state,
    address_object['country']::string as billing_country,
    address_object['line1']::string as billing_address_line_1,
    address_object['line2']::string as billing_address_line_2,
    address_object['postal_code']::string as billing_postal_code,

    parse_json(billing_details)['email'] as stripe_customer_email,
    {{dbt_utils.surrogate_key(['stripe_customer_email'])}} as stripe_customer_email_sk
    
from source
)

select * from renamed