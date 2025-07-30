with archived_client_notes as (

    select * from {{ source('app_archive', 'client_notes') }}

)

select * from archived_client_notes