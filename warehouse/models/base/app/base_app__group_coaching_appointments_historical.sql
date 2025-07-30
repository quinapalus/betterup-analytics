with archived_group_coaching_appointments as (

    select * from {{ source('app_archive', 'group_coaching_appointments') }}

)

select * from archived_group_coaching_appointments