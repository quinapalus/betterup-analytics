with archived_active_storage_attachments as (

    select * from {{ source('app_archive', 'active_storage_attachments') }}

)

select * from archived_active_storage_attachments