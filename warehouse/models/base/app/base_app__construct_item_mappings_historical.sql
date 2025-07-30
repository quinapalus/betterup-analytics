with archived_construct_item_mappings as (

    select * from {{ source('app_archive', 'construct_item_mappings') }}

)

select * from archived_construct_item_mappings