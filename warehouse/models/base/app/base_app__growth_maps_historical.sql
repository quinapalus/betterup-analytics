with archived_growth_maps as (

    select * from {{ source('app_archive', 'growth_maps') }}

)

select * from archived_growth_maps