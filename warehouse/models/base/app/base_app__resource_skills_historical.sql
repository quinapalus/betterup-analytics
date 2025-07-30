with archived_resource_skills as (

    select * from {{ source('app_archive', 'resource_skills') }}

)

select * from archived_resource_skills