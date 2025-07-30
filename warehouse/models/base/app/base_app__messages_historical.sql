with archived_messages as (

    select * from {{ source('app_archive', 'messages') }}

)

select * from archived_messages