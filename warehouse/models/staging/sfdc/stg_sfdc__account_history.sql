WITH account_history AS (

  SELECT * FROM {{ source('salesforce', 'account_history') }}

)


SELECT
  id AS sfdc_account_history_id,
  account_id AS sfdc_account_id,
  field,
  new_value,
  old_value,
  is_deleted,
  {{ load_timestamp('created_date', alias='created_at') }}
FROM account_history
