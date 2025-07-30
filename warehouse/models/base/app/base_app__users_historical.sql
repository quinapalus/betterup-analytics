with archived_users as (

    select * from {{ source('app_archive', 'users') }}

)

select * from archived_users