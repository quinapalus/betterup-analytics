with m49_geoscheme as (

    select * from {{ ref('m49_geoscheme') }}

)

select * from m49_geoscheme

