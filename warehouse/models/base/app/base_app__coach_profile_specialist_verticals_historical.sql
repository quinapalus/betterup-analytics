with archived_coach_profile_specialist_verticals as (

    select * from {{ source('app_archive', 'coach_profile_specialist_verticals') }}

)

select * from archived_coach_profile_specialist_verticals