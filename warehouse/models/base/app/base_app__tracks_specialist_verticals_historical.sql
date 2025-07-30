with archived_tracks_specialist_verticals as (

    select * from {{ source('app_archive', 'tracks_specialist_verticals') }}

)

select * from archived_tracks_specialist_verticals