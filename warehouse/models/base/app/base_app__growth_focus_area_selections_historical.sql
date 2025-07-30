with archived_growth_focus_area_selections as (

    select * from {{ source('app_archive', 'growth_focus_area_selections') }}

)

select * from archived_growth_focus_area_selections