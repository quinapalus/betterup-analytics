{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH member_attributes AS (

  SELECT * FROM {{ref('dei_member_attributes_sanitized')}}
  -- Filter to only include fields that have been whitelisted in config file:
  WHERE category_name IN (
    SELECT internal_field_name
    FROM {{ref('carina_hcm_attribute_fields')}}
  )

),

accounts AS (

  SELECT * FROM {{ref('dei_accounts')}}

)


SELECT
  -- Surrogate Primary Key of member_key, account_key, and internal_field_name:
  {{ dbt_utils.surrogate_key(['ma.member_id', 'a.organization_id', 'a.sfdc_account_id', 'category_name']) }} AS id,
  {{ member_key('ma.member_id') }} AS member_key,
  {{ account_key('a.organization_id', 'a.sfdc_account_id') }} AS account_key,
  category_name AS internal_field_name,
  category_name_partner AS hcm_field_name,
  value
FROM member_attributes AS ma
INNER JOIN accounts AS a
  ON ma.organization_id = a.organization_id
