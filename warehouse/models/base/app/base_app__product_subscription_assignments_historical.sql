with archived_product_subscription_assignments as (

    select * from {{ source('app_archive', 'product_subscription_assignments') }}

)

select * from archived_product_subscription_assignments