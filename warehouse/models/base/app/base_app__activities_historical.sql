with archived_activities as (

    select * from {{ source('app_archive', 'activities') }}

)

select * from archived_activities