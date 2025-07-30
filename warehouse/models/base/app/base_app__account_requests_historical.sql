with archived_account_requests as (

    select * from {{ source('app_archive', 'account_requests') }}

)

select * from archived_account_requests