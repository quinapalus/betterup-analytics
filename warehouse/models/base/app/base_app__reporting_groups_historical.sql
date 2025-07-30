with archived_reporting_groups as (

    select * from {{ source('app_archive', 'reporting_groups') }}

)

select * from archived_reporting_groups