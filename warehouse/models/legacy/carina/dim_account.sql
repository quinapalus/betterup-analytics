{{
  config(
    tags=['classification.c3_confidential','eu']
  )
}}

WITH accounts AS (

  SELECT * FROM {{ref('dei_accounts')}}

)


SELECT
  {{ account_key('organization_id', 'sfdc_account_id') }} AS account_key,
  organization_name AS app_organization_name,
  sfdc_account_name,
  sfdc_account_owner,
  sfdc_account_csm,
  COALESCE(account_segment, 'Unknown') AS account_segment,
  industry AS account_industry,
  company_size AS account_company_size,
  sfdc_account_id,
  organization_id AS app_organization_id
FROM accounts
WHERE organization_id <> 1 -- exclude BetterUp
