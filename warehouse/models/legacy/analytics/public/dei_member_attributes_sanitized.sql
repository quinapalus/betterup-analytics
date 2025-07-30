WITH member_attributes AS (

  SELECT * FROM {{ref('dbt_member_attributes')}}

),

member_count_per_attribute_value AS (

  SELECT
    organization_id,
    category_name,
    -- carry through original attribute value to enable backtracking logic
    value,
    -- for data privacy, mask original attribute values with insufficient number of members
    CASE
      WHEN COUNT(member_id) < 5 THEN 'other' ELSE value
    END AS masked_value,
    COUNT(member_id) AS member_count
  FROM member_attributes
  GROUP BY organization_id, category_name, value

),

member_sum_per_attribute_value AS (

  SELECT
    organization_id,
    category_name,
    masked_value,
    -- add another layer of aggregation so that masked attribute values
    -- can be grouped together
    SUM(member_count) AS member_sum
  FROM member_count_per_attribute_value
  GROUP BY organization_id, category_name, masked_value

),

sanitized_internal_categories_per_org AS (

  SELECT
    organization_id,
    category_name
  FROM member_sum_per_attribute_value
  -- exclude those categories whose grouped masked attribute
  -- values do not have a sufficient number of members.
  EXCEPT
  SELECT
    organization_id,
    category_name
  FROM member_sum_per_attribute_value
  WHERE (masked_value = 'other' AND member_sum < 5)

)


SELECT
  -- Surrogate Primary Key of Member_id, Organization_id, Category_name_partner, Category_name
  {{ dbt_utils.surrogate_key(['ma.member_id', 'ma.organization_id', 'ma.category_name_partner', 'ma.category_name']) }} AS id,
  ma.member_id,
  ma.organization_id,
  ma.category_name_partner,
  ma.category_name,
  mc.masked_value AS value
FROM member_attributes AS ma
-- backtrack to initial CTE to mask necessary attribute values
INNER JOIN member_count_per_attribute_value AS mc
  ON ma.organization_id = mc.organization_id
  AND ma.category_name = mc.category_name
  AND ma.value = mc.value
-- choose only sanitized combinations of organization-internal category
WHERE (ma.organization_id, ma.category_name) IN (

  SELECT
    organization_id,
    category_name
  FROM sanitized_internal_categories_per_org

)
