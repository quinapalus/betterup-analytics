with opportunity_field_history as (

  select * from {{ source('salesforce', 'opportunity_field_history') }}

)


select
  id as sfdc_opportunity_field_history_id,
  opportunity_id as sfdc_opportunity_id,
  field,
  new_value,
  old_value,
  is_deleted,
  {{ load_timestamp('created_date', alias='created_at') }}
from opportunity_field_history
