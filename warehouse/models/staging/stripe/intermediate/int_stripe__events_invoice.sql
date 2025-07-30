with source as (
--temporarily pointing to the new stripe events table before deprecating the old one.
  select * from {{ ref('stg_stripe__events_new')}}
  where lower(event_type) like '%invoice%'
),

parsing_json as (
    select
    --primary key
        stripe_event_id,

    --foreign key
        data_object['id']::string stripe_invoice_id,
        data_object['customer']::string stripe_customer_id,
        data_object['subscription']::string stripe_subscription_id,
        data_object['charge']::string stripe_charge_id,


    --parse JSON for Invoice Related Events
        event_type,
        data_object['billing_reason']::string as billing_reason,
        data_object['currency']::string as currency,
        data_object['amount_due']/100.00 as amount_due,
        data_object['amount_paid']/100.00 as amount_paid,
        data_object['amount_remaining']/100.00 as amount_remaining,

    --timestamps
        data_object['created']::timestamp_ntz as created_at,
        data_object['finalized_at']::timestamp_ntz as finalized_at,

    --misc
        is_livemode,
        api_version,
        data

    from source
)

select * from parsing_json