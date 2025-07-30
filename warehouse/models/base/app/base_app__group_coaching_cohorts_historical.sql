with archived_group_coaching_cohorts as (

    select * from {{ source('app_archive', 'group_coaching_cohorts') }}

)

select * from archived_group_coaching_cohorts