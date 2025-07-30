with archived_coach_assignments as (

    select * from {{ source('app_archive', 'coach_assignments') }}

)

select * from archived_coach_assignments