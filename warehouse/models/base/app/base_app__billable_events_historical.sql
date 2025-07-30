with archived_billable_events as (

    select * from {{ source('app_archive', 'billable_events') }}

)

select * from archived_billable_events