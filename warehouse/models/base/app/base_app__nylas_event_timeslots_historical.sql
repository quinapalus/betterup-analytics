with archived_nylas_event_timeslots as (

    select * from {{ source('app_archive', 'nylas_event_timeslots') }}

)

select * from archived_nylas_event_timeslots