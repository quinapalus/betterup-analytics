with archived_pause_intervals as (

    select * from {{ source('app_archive', 'pause_intervals') }}

)

select * from archived_pause_intervals