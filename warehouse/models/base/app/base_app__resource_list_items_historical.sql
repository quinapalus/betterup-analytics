with archived_resource_list_items as (

    select * from {{ source('app_archive', 'resource_list_items') }}

)

select * from archived_resource_list_items