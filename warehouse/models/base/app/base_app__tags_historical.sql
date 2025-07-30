with archived_tags as (

    select * from {{ source('app_archive', 'tags') }}

)

select * from archived_tags