with geo_categories as (

    select * from {{ ref('bu_geo_categories') }}

)

select * from geo_categories

