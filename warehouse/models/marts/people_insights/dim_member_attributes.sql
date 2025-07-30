{{
  config(
    tags=['eu']
  )
}}

WITH user_attributes AS (

  SELECT * FROM {{ ref('stg_app__user_attributes') }}

),

user_attribute_values AS (

  SELECT * FROM {{ ref('stg_app__user_attribute_values') }}

),

user_attribute_fields AS (

  SELECT * FROM {{ ref('stg_app__user_attribute_fields') }}

),

user_attribute_internal_fields AS (

  SELECT * FROM {{ ref('stg_app__user_attribute_internal_fields') }}

)


SELECT
  ua.user_attribute_id,
  ua.user_id AS member_id,
  ua.user_attribute_field_id,
  uaf.field_name,
  coalesce(uaf.display_name,uaf.field_name) AS display_field_name,
  uaif.field_name AS internal_field_name,
  uaif.description AS internal_field_description,
  uaf.organization_id,
  uaf.filterable,
  uaf.exportable,
  uav.user_attribute_value_id,
  uav.attribute_value,
  ua.created_at,
  ua.updated_at
FROM user_attributes AS ua
INNER JOIN user_attribute_values AS uav
  ON ua.user_attribute_value_id = uav.user_attribute_value_id
INNER JOIN user_attribute_fields AS uaf
  ON ua.user_attribute_field_id = uaf.user_attribute_field_id
LEFT OUTER JOIN user_attribute_internal_fields AS uaif
  ON uaf.user_attribute_internal_field_id = uaif.user_attribute_internal_field_id
WHERE (NOT uaf.archived OR uaf.archived IS NULL)
QUALIFY
  -- filter for most recent attribute value for each member/field pair
  ROW_NUMBER() OVER (PARTITION BY ua.user_id, ua.user_attribute_field_id ORDER BY ua.created_at DESC) = 1
  -- "soft delete" any attributes where most recent value is set to NULL
  AND uav.attribute_value IS NOT NULL
