with mapping as (

   select * from {{ source('gsheets_coach_onboarding_inputs', 'mapping__stage_buckets') }}

)

select
    {{ dbt_utils.surrogate_key(['stage_name_prefix', 'funnel_id']) }} as primary_key, 
    stage_name_prefix,
    replace(lower(bucket_name),' ','_') as bucket_name, --removing spaces and converting to lowercase
    funnel_name,
    funnel_id,
    funnel_order
from mapping
