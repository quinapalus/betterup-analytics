with archived_resource_assets as (

    select * from {{ source('app_archive', 'resource_assets') }}

)

select * from archived_resource_assets