with archived_track_assignment_specialist_verticals as (

    select * from {{ source('app_archive', 'track_assignment_specialist_verticals') }}

)

select * from archived_track_assignment_specialist_verticals