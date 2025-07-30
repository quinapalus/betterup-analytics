with archived_taggings as (

    select * from {{ source('app_archive', 'taggings') }}

)

select * from archived_taggings