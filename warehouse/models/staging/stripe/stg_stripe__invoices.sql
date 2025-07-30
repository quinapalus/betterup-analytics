with source as (
  select * from {{ source('stripe', 'invoices') }}
),

renamed as (
select 
    --primary key
    id as stripe_invoice_id,

    --foreign keys
    charge as stripe_charge_id,
    customer as stripe_customer_id,
    customer_email,
    payment_intent as stripe_payment_intent_id,
    subscription as stripe_subscription_id,
    discount['coupon']['id']::string as stripe_coupon_id,

    --attributes
    amount_due,
    amount_paid,
    amount_remaining,
    subtotal,
    total,
    currency,
    customer_tax_exempt,
    paid as is_paid,

    --misc
    closed as is_closed,
    collection_method,
    attempted as has_attempted,
    attempt_count,
    auto_advance as is_auto_advance,
    billing,
    billing_reason,
    account_country,
    account_name,
    customer_tax_ids,
    forgiven as is_forgiven,
    hosted_invoice_url,
    invoice_pdf,
    lines,
    livemode as is_livemode,
    number,
    object,
    receipt_number,
    status,
    discount as discount_object,    

    --timestamp
    date::timestamp_ntz as timestamp,
    created::timestamp_ntz as created_at,
    updated::timestamp_ntz as updated_at,
    finalized_at::timestamp_ntz as finalized_at,
    period_end::timestamp_ntz as period_end_at,
    period_start::timestamp_ntz as period_start_at,
    webhooks_delivered_at::timestamp_ntz as webhooks_delivered_at
from source
)

select * from renamed