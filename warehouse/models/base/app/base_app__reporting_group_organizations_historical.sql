with archived_reporting_group_organizations as (

    select * from {{ source('app_archive', 'reporting_group_organizations') }}

)

select * from archived_reporting_group_organizations