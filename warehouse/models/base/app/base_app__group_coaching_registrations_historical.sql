with archived_group_coaching_registrations as (

    select * from {{ source('app_archive', 'group_coaching_registrations') }}

)

select * from archived_group_coaching_registrations