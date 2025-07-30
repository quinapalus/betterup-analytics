with archived_appointments as (

    select * from {{ source('app_archive', 'appointments') }}

)

select * from archived_appointments