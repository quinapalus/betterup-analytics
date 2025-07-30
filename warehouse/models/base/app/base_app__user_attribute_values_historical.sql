with archived_user_attribute_values as (

    select * from {{ source('app_archive', 'user_attribute_values') }}

)

select * from archived_user_attribute_values