{{
  config(
    materialized='view'
  )
}}

WITH product_recommendations AS (
    SELECT * FROM {{ source('identify', 'product_recommendations') }}
)

SELECT user_id,
       product_id,
       id,
       created_at,
       updated_at,
       score,
       model_version
FROM product_recommendations
