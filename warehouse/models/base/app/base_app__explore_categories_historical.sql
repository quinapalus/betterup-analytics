with archived_explore_categories as (

    select * from {{ source('app_archive', 'explore_categories') }}

)

select * from archived_explore_categories