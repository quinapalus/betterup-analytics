with source as (
    select * from {{ ref('stg_stripe__invoice_items')}}
),

final as (
    select
    --primary key
        stripe_invoice_item_id,

    --foreign keys
        stripe_customer_id,
        stripe_invoice_id,
        stripe_plan_id,
        stripe_product_id,

    --attributes
        amount,
        object,
        description,
        is_discountable,

    --timestamps
        ended_at,
        started_at
    from source
)

select * from final