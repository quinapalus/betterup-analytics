{{
  config(
    materialized='view'
  )
}}

WITH product_recommendation_attributes AS (
    SELECT * FROM {{ source('identify', 'product_recommendation_attributes') }}
)

SELECT user_id,
       product_id,
       product_recommendation_id,
       attribute_name,
       value_float,
       value_string,
       attribute_type,
       id,
       created_at,
       updated_at
FROM product_recommendation_attributes
