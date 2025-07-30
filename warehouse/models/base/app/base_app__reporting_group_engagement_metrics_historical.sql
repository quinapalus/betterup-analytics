with archived_reporting_group_engagement_metrics as (

    select * from {{ source('app_archive', 'reporting_group_engagement_metrics') }}

)

select * from archived_reporting_group_engagement_metrics