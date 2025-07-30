with archived_timeslots as (

    select * from {{ source('app_archive', 'timeslots') }}

)

select * from archived_timeslots