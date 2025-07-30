with archived_notes as (

    select * from {{ source('app_archive', 'notes') }}

)

select * from archived_notes