{{
  config(
    tags=["eu"]
  )
}}

WITH reporting_group_organizations AS (

  SELECT * FROM  {{ ref('int_app__reporting_group_organizations') }}

)

SELECT
  reporting_group_organization_id,
  reporting_group_id,
  organization_id,
  created_at,
  updated_at
FROM reporting_group_organizations
