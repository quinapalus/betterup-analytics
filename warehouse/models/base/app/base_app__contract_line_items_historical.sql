with archived_contract_line_items as (

    select * from {{ source('app_archive', 'contract_line_items') }}

)

select * from archived_contract_line_items