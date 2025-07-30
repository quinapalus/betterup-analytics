with archived_user_attribute_records as (

    select * from {{ source('app_archive', 'user_attribute_records') }}

)

select * from archived_user_attribute_records