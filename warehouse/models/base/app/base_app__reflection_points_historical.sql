with archived_reflection_points as (

    select * from {{ source('app_archive', 'reflection_points') }}

)

select * from archived_reflection_points