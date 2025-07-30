with archived_groups as (

    select * from {{ source('app_archive', 'groups') }}

)

select * from archived_groups