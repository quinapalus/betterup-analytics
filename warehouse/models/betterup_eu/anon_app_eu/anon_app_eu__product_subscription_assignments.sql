WITH src_eu_product_subscription_assignments AS ( 
    SELECT * FROM {{ source('app_eu', 'product_subscription_assignments') }}
),
product_subscription_assignments_eu AS ( 
    SELECT 
    CHANGE_NOTIFICATIONS_ENABLED ,
    CREATED_AT,
    ENDS_AT,
    ID,
    PRODUCT_SUBSCRIPTION_ID,
    STARTED_AT,
    STARTS_AT,
    UPDATED_AT,
    USER_ID,
    V2        
    FROM src_eu_product_subscription_assignments
)

SELECT * FROM product_subscription_assignments_eu