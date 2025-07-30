with archived_growth_plan_items as (

    select * from {{ source('app_archive', 'growth_plan_items') }}

)

select * from archived_growth_plan_items