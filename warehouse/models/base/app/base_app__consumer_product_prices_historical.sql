with archived_consumer_product_prices as (

    select * from {{ source('app_archive', 'consumer_product_prices') }}

)

select * from archived_consumer_product_prices