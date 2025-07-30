with archived_account_authorizations as (

    select * from {{ source('app_archive', 'account_authorizations') }}

)

select * from archived_account_authorizations