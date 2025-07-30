with archived_resource_development_topics as (

    select * from {{ source('app_archive', 'resource_development_topics') }}

)

select * from archived_resource_development_topics