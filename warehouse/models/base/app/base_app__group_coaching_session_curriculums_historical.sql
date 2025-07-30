with archived_group_coaching_session_curriculums as (

    select * from {{ source('app_archive', 'group_coaching_session_curriculums') }}

)

select * from archived_group_coaching_session_curriculums