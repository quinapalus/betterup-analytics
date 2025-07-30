with archived_roles as (

    select * from {{ source('app_archive', 'roles') }}

)

select * from archived_roles