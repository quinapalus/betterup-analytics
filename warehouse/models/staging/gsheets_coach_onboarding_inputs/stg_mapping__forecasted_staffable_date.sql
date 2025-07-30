with mapping as (

   select * from {{ source('gsheets_coach_onboarding_inputs', 'mapping__forecasted_staffable_date') }}

)

select
    {{ dbt_utils.surrogate_key(['bucket_name', 'funnel_id','forecast_start_date']) }} as primary_key, 
    replace(lower(bucket_name),' ','_') as bucket_name, --removing spaces and converting to lowercase,
    funnel_name,
    funnel_id,
    expected_days_in_bucket,
    forecast_start_date,
    forecast_end_date
from mapping
