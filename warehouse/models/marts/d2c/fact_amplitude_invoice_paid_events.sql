with source as (
    select * from {{ ref('int_amplitude__invoice_events')}}
),

renamed as (
    select
        
        --primary key
        uuid,

        --foreign keys
        user_id,
        stripe_charge_id,
        stripe_subscription_id,
        stripe_product_id,
        product_subscription_id,
        product_subscription_assignment_id,
        consumer_product_id,
        product_id,
        track_id,
        track_assignment_id,


        --user attributes at time of event
        --timestamps
        event_time,

        --other
        deployment_type,
        track_deployment_type,
        billing_reason,
        amount_paid

    from source
)

select * from renamed