with archived_organization_contacts as (

    select * from {{ source('app_archive', 'organization_contacts') }}

)

select * from archived_organization_contacts