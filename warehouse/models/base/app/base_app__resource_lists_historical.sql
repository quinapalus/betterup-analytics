with archived_resource_lists as (

    select * from {{ source('app_archive', 'resource_lists') }}

)

select * from archived_resource_lists