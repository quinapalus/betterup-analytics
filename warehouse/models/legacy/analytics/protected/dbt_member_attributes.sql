WITH user_attributes AS (

  SELECT * FROM {{ref('stg_app__user_attributes')}}

),

user_attribute_fields AS (

  SELECT * FROM {{ref('stg_app__user_attribute_fields')}}

),

user_attribute_internal_fields AS (

  SELECT * FROM {{ref('stg_app__user_attribute_internal_fields')}}

)

SELECT * FROM (

SELECT
  a.user_id AS member_id,
  f.organization_id,
  f.field_name AS category_name_partner,
  i.field_name AS category_name,
  a.value,
  ROW_NUMBER() OVER (
      PARTITION BY a.user_id, a.user_attribute_field_id
      ORDER BY a.created_at DESC
  ) AS index
FROM user_attributes AS a
JOIN user_attribute_fields AS f
  ON a.user_attribute_field_id = f.user_attribute_field_id
JOIN user_attribute_internal_fields AS i
  ON f.user_attribute_internal_field_id = i.user_attribute_internal_field_id
WHERE
  -- do not surface empty attribute values
  a.value IS NOT NULL
  -- surface only the most recent value for each user-field pair

) a

WHERE index = 1
