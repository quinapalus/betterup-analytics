with archived_consumer_products as (

    select * from {{ source('app_archive', 'consumer_products') }}

)

select * from archived_consumer_products