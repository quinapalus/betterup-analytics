with archived_group_coaching_series_tracks as (

    select * from {{ source('app_archive', 'group_coaching_series_tracks') }}

)

select * from archived_group_coaching_series_tracks