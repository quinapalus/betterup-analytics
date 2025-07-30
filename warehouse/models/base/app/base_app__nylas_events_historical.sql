with archived_nylas_events as (

    select * from {{ source('app_archive', 'nylas_events') }}

)

select * from archived_nylas_events