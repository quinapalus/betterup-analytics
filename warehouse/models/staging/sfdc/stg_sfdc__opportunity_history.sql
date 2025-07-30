WITH opportunity_history AS (

  SELECT * FROM {{ source('salesforce', 'opportunity_history') }}

)


SELECT
  id AS sfdc_opportunity_history_id,
  opportunity_id AS sfdc_opportunity_id,
  {{ load_timestamp('created_date', alias='created_at') }},
  amount,
  stage_name,
  {{ load_timestamp('close_date', alias='close_date') }},
  is_deleted
FROM opportunity_history
WHERE NOT is_deleted
