with source as (
    select * from {{ source('stripe', 'subscription_items') }}
),

renamed as (
    select

        --primary key
        id as stripe_subscription_item_id,
        
        --foreign keys
        subscription as stripe_subscription_id,
        plan['id']::string as stripe_plan_id,
        plan['product']::string as stripe_product_id,

        --attributes
        --timestamps
        created::timestamp_ntz as created_at,

        --metadata
        object,
        quantity,
        metadata

    from source

)

select * from renamed