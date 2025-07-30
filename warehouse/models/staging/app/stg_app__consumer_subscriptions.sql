with source as (
    select * from {{ source('app', 'consumer_subscriptions') }}
),

renamed as (
    select

    -- primary key
        id as app_subscription_id,

    --foreign key
        stripe_subscription_id,
        track_assignment_id,

    -- timestamps
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }},
        {{ load_timestamp('ended_at') }},
        {{ load_timestamp('trial_ends_on') }},
        {{ load_timestamp('trial_ended_at') }},

    --other
        stripe_data,
        hidden as is_hidden,
        number_of_invoices

    from source
)

select * from renamed