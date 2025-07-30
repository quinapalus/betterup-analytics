with archived_activity_notes as (

    select * from {{ source('app_archive', 'activity_notes') }}

)

select * from archived_activity_notes