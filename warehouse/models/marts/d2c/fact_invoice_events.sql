with invoice_events as (
    select * from {{ ref('int_stripe__events_invoice')}}
),

renamed as (
    select
    --primary key
        stripe_event_id,

    --foreign key
        stripe_invoice_id,
        stripe_customer_id,
        stripe_subscription_id,
        stripe_charge_id,

    --attributes
        event_type,
        billing_reason,
        currency,
        amount_due,
        amount_paid,
        amount_remaining,

    ----timestamps
        created_at,
        finalized_at

    from invoice_events
)


select * from renamed
