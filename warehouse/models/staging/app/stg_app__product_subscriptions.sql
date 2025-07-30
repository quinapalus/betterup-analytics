{{
  config(
    tags=['eu']
  )
}}

with source as (
    select * from {{ source('app', 'product_subscriptions') }}
),
destroyed_records as (
    select * 
    from {{ ref('stg_app__versions_delete')}}
    where item_type = 'ProductSubscription'
),
filtered as (
    select s.*
    from source s
    left join destroyed_records as v 
        on s.id = v.item_id
    where v.item_id is null
),
renamed as (
    select
        --Primary Key
        id as product_subscription_id,

        --Foreign Keys
        product_id,
        organization_id,
        subscription_terms_id,

        --Logical data
        state,
        name,

        --Timestamps
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}
        
    from filtered

)

select * from renamed
