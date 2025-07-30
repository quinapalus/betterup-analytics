with stages as (

    select * from {{ ref('stg_fountain__stages') }}

),

stage_buckets as (

    select * from {{ ref('stg_mapping__stage_buckets') }}

)

select
    stages.*,
    stage_buckets.bucket_name,
    stage_buckets.funnel_order
from stages 
left join stage_buckets
    on stage_buckets.stage_name_prefix = stages.stage_name_prefix
       and stage_buckets.funnel_id = stages.funnel_id
