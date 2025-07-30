with archived_group_coaching_curriculum_skills as (

    select * from {{ source('app_archive', 'group_coaching_curriculum_skills') }}

)

select * from archived_group_coaching_curriculum_skills