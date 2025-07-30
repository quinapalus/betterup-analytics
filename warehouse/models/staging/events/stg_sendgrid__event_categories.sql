with sendgrid_event_categories_source as (

  SELECT * FROM {{ source('sendgrid', 'event_categories') }}
  
),

sendgrid_event_categories_renamed as (
    
    select 

    "_sdc_batched_at" as _sdc_batched_at,
    "_sdc_level_0_id" as _sdc_level_0_id,
    "_sdc_received_at" as _sdc_received_at,
    "_sdc_sequence" as _sdc_sequence,
    "_sdc_source_key_email" as _sdc_source_key_email,
    "_sdc_source_key_event" as _sdc_source_key_event,
    "_sdc_source_key_timestamp" as _sdc_source_key_timestamp,
    "_sdc_table_version" as _sdc_table_version,
    "value" as value
     
    from sendgrid_event_categories_source
    
)

select
  -- primary_key
  {{ dbt_utils.surrogate_key(['_sdc_source_key_timestamp', '_sdc_level_0_id', '_sdc_source_key_email', '_sdc_source_key_event']) }} as primary_key,
  *
from sendgrid_event_categories_renamed