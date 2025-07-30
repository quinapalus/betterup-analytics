{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH dim_date AS (

  SELECT * FROM {{ref('dim_date')}}

)

SELECT DISTINCT
{{ month_key('date') }} AS month_key,
{{ dbt_utils.star(from=ref('dim_date'), except=["DATE_KEY", "DATE"]) }}
FROM {{ref('dim_date')}}
