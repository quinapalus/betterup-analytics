with archived_user_groups as (

    select * from {{ source('app_archive', 'user_groups') }}

)

select * from archived_user_groups