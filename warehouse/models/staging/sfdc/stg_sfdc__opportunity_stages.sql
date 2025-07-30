
with opportunity_stages as (

    select * from {{ source('salesforce','opportunity_stage') }}

)

select 
id as sfdc_opportunity_stage_id,
api_name,
master_label,
is_active,
is_closed,
is_won,
forecast_category_name,
description
from opportunity_stages