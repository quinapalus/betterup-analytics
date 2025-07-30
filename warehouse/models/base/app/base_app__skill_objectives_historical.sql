with archived_skill_objectives as (

    select * from {{ source('app_archive', 'skill_objectives') }}

)

select * from archived_skill_objectives