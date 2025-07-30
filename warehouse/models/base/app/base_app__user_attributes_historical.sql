with archived_user_attributes as (

    select * from {{ source('app_archive', 'user_attributes') }}

)

select * from archived_user_attributes