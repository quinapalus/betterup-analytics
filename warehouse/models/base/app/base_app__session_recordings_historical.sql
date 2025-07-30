with archived_session_recordings as (

    select * from {{ source('app_archive', 'session_recordings') }}

)

select * from archived_session_recordings