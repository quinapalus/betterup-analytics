select 

    id as funnel_id,
    {{ dbt_utils.surrogate_key(['id','dbt_valid_from','dbt_valid_to']) }} as primary_key,
    title as funnel_title,
    active as is_active_funnel,

    --other
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    dbt_valid_to is null as is_current_version,
    
    row_number() over(
      partition by id
      order by dbt_valid_from
    ) as version,
      
    case when
      row_number() over(
        partition by id,date_trunc('day',dbt_valid_from)
        order by dbt_valid_from desc
      ) = 1 then true else false end as is_last_snapshot_of_day
  
from {{ ref('snapshot_fountain_funnels') }}
