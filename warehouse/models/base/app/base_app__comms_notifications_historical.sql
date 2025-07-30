with archived_comms_notifications as (

    select * from {{ source('app_archive', 'comms_notifications') }}

)

select * from archived_comms_notifications