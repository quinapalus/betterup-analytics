with reporting_group_engagement_metrics as (

  select * from {{ ref('stg_app__reporting_group_engagement_metrics') }}

)

select
    care,
    coaching_circles,
    created_at,
    ended_at,
    extended_network,
    foundations,
    reporting_group_engagement_metric_id,
    last_engaged_at,
    member_id,
    next_session_at,
    on_demand,
    primary_coaching,
    product_access,
    reporting_group_id,
    status,
    total_sessions,
    track_name,
    updated_at,
    workshops,
    hidden,
    started_at,
    last_session_at
from reporting_group_engagement_metrics
