with archived_nylas_accounts as (

    select * from {{ source('app_archive', 'nylas_accounts') }}

)

select * from archived_nylas_accounts