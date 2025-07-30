with archived_objectives as (

    select * from {{ source('app_archive', 'objectives') }}

)

select * from archived_objectives