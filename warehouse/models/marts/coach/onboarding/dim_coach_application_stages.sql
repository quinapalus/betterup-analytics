with application_stages as (

    select * from {{ ref('int_fountain__stages') }}

)

select 
    funnel_id,
    stage_id,
    stage_title,
    stage_type,
    bucket_name,
    funnel_order
from application_stages
