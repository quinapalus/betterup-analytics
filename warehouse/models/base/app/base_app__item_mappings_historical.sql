with archived_item_mappings as (

    select * from {{ source('app_archive', 'item_mappings') }}

)

select * from archived_item_mappings