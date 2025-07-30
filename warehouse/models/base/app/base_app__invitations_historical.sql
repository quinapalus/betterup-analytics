with archived_invitations as (

    select * from {{ source('app_archive', 'invitations') }}

)

select * from archived_invitations