with archived_coach_recommendations as (

    select * from {{ source('app_archive', 'coach_recommendations') }}

)

select * from archived_coach_recommendations