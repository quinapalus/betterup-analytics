with archived_milestones as (

    select * from {{ source('app_archive', 'milestones') }}

)

select * from archived_milestones