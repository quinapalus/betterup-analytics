with archived_billable_event_imports as (

    select * from {{ source('app_archive', 'billable_event_imports') }}

)

select * from archived_billable_event_imports
