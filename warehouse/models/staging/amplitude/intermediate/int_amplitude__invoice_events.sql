with subset_events as (
    select * from {{ ref('int_amplitude__events')}}
    where lower(event_type) like '%invoice%'
),

parsed_events as (
    select
        *,

        --extracting metadata from event properties
        event_properties['product_subscription_id']::string as product_subscription_id,
        event_properties['product_subscription_assignment_id']::string as product_subscription_assignment_id,
        event_properties['subscription_id']::string as stripe_subscription_id,
        event_properties['charge_id']::string as stripe_charge_id,
        event_properties['consumer_product.id'] as consumer_product_id,
        event_properties['product_id']::string as product_id,
        event_properties['consumer_product.stripe_product_id']::string as stripe_product_id,
        event_properties['track_id']::string as track_id,
        event_properties['track_assignment_id']::string as track_assignment_id,

        event_properties['deployment_type'] as deployment_type,
        event_properties['billing_reason']::string as billing_reason,
        event_properties['amount_paid']::numeric as amount_paid

    from subset_events
)

select * from parsed_events