with archived_growth_focus_area_skills as (

    select * from {{ source('app_archive', 'growth_focus_area_skills') }}

)

select * from archived_growth_focus_area_skills