with archived_track_assignments as (

    select * from {{ source('app_archive', 'track_assignments') }}

)

select * from archived_track_assignments