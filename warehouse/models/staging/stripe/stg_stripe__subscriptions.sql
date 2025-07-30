with source as (
    select * from {{ source('stripe', 'subscriptions') }}
),

renamed as (
    select

        --primary key
        id as stripe_subscription_id,
        
        --foreign keys
        customer as stripe_customer_id,
        items as stripe_subscription_item_id_array,
        latest_invoice as stripe_latest_invoice_id,
        plan['id']::string as stripe_plan_id,
        plan['product']::string as stripe_product_id,

        --attributes
        /*will help us catch edge cases where subscription has multiple subscription items */
        array_size(items) as count_of_subscription_item,
        days_until_due,

        --billing
        plan as plan_object,
        billing,
        billing_cycle_anchor,
        collection_method,
        default_payment_method,
        discount,
        invoice_customer_balance_settings,
        cancel_at_period_end as is_cancel_at_period_end,

        --timestamps
        created::timestamp_ntz as created_at,
        start_date::timestamp_ntz as started_at,
        ended_at::timestamp_ntz as ended_at,
        canceled_at::timestamp_ntz as canceled_at,
        cancel_at,
        current_period_end::timestamp_ntz as current_period_end_at,
        current_period_start::timestamp_ntz as current_period_start_at,
        trial_end::timestamp_ntz as trial_end_at,
        trial_start::timestamp_ntz as trial_start_at,
        updated::timestamp_ntz as updated_at,

        --metadata
        livemode as is_livemode,
        metadata,
        object,
        quantity,
        "START", /*start is a snowflake keyword so we have to format it like this.
          it should be renamed but currently is not used past this model and it's 
          definition is not clear. */
        status,
        pause_collection,
        default_source

    from source

)

select * from renamed