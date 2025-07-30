with archived_group_coaching_sessions as (

    select * from {{ source('app_archive', 'group_coaching_sessions') }}

)

select * from archived_group_coaching_sessions