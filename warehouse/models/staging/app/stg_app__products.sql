with source as (
    select * from {{ source('app', 'products') }}
),

destroyed_records as (
    select * 
    from {{ ref('stg_app__versions_delete') }}
    where item_type = 'Product'
),

renamed as (

    select
        --Primary Key
        id as product_id,
        uuid as product_uuid,
        salesforce_product_identifier,

        --Logical data
        name,
        description,
        product_type,
        product_family,
        product_code,
        source,
        pricing_model,
        specialist_catalog,
        specialist_coaching_limited,
        sessions_per_month,
        care,
        care_limited,
        coaching_circles,
        coaching_circles_limited,
        coaching_circles_limit,
        extended_network,
        on_demand,
        primary_coaching,
        primary_coaching_limited,
        primary_solution,
        workshops,
        workshop_catalog,
        coalesce(coaching_cloud,'unlisted') as coaching_cloud,
        {{ sanitize_product_group('on_demand','primary_coaching','care','coaching_circles','workshops','extended_network') }} as product_group,
        
        --Timestamps
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}

    from source
    left join destroyed_records
        on source.id = destroyed_records.item_id
    where destroyed_records.item_id is null

)

select * from renamed