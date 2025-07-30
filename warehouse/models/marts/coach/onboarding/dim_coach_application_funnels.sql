with application_funnels as (

    select * from {{ ref('stg_fountain__funnels_snapshot') }}
    where is_current_version

)

select
    funnel_id,
    funnel_title,
    is_active_funnel
from application_funnels
