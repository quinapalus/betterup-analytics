with archived_linked_accounts as (

    select * from {{ source('app_archive', 'linked_accounts') }}

)

select * from archived_linked_accounts