WITH reporting_groups AS (

  SELECT * FROM  {{ ref('int_app__reporting_groups') }}

)

SELECT
  reporting_group_id,
  name AS reporting_group_name,
  product_type,
  created_at,
  updated_at
FROM reporting_groups
