with archived_contracts as (

    select * from {{ source('app_archive', 'contracts') }}

)

select * from archived_contracts