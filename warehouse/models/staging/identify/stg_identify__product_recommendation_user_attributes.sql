{{
  config(
    materialized='view'
  )
}}

WITH product_recommendation_attributes AS (
    SELECT * FROM {{ source('identify', 'product_recommendation_user_attributes') }}
)

SELECT
       {{ dbt_utils.surrogate_key(['id', 'user_id', 'attribute_name']) }} as unique_id,
       user_id,
       created_at,
       updated_at,
       id,
       value_float,
       value_string,
       attribute_name,
       attribute_type
FROM product_recommendation_attributes
