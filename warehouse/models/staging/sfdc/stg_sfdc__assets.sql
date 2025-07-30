with source as (

    select * from {{ source('salesforce', 'assets') }}

),

renamed as (

    select
        id as asset_id,
        is_deleted,
        name,
        owner_id,
        system_modstamp,
        uuid_ts,
        last_modified_by_id,
        account_id,
        created_by_id,
        created_date,
        is_competitor_product,
        product_2_id,
        quantity,
        last_modified_date,
        price,
        received_at,
        root_asset_id,
        last_referenced_date,
        last_viewed_date

    from source

)

select * from renamed
