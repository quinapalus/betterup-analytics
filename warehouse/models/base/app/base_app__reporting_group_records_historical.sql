with archived_reporting_group_records as (

    select * from {{ source('app_archive', 'reporting_group_records') }}

)

select * from archived_reporting_group_records