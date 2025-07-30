with archived_second_versions as (

    select * from {{ source('app_archive', 'second_versions') }}

)

select * from archived_second_versions