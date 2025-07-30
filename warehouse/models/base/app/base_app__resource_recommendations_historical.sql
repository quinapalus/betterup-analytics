with archived_resource_recommendations as (

    select * from {{ source('app_archive', 'resource_recommendations') }}

)

select * from archived_resource_recommendations