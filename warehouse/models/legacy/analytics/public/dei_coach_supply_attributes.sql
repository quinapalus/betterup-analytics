WITH coach_supply AS (

  SELECT * FROM {{ref('dei_coach_supply')}}

)

SELECT DISTINCT
  attribute_scope,
  attribute_value,
  attribute_label
FROM coach_supply
ORDER BY attribute_scope, attribute_value
