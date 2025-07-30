with archived_product_subscription_assignment_migration_audits as (

    select * from {{ source('app_archive', 'product_subscription_assignment_migration_audits') }}

)

select * from archived_product_subscription_assignment_migration_audits