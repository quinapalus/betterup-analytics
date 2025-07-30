with user_snapshot as (

    select * from {{ ref('snapshot_sfdc_users') }}
)

select 
    --ids
    id as sfdc_user_id,
    {{ dbt_utils.surrogate_key(['id','dbt_valid_from','dbt_valid_to']) }} as history_primary_key,
    manager_id as manager_id,
    assigned_sdr_c as assigned_sdr_id,
    employee_id_c as employee_id,
    user_role_id,
    rvp_c as rvp_id,
    vp_c as vp_id,
  
    --categorical and text attributes
    name,
    sales_region_c as sales_region,
    title,
    
    --quantities
    
    --boooleans
  
    --dates and timestamps
    {{ environment_varchar_to_timestamp('created_date','created_at') }},
    {{ environment_varchar_to_timestamp('last_modified_date','last_modified_at') }},
    {{ environment_varchar_to_timestamp('dbt_valid_from','valid_from') }},
    {{ environment_varchar_to_timestamp('dbt_valid_to','valid_to') }},

    --other
    is_active,
    dbt_valid_to is null as is_current_version,
    row_number() over(
      partition by id
      order by dbt_valid_from
    ) as version,

    case when
      row_number() over(
        partition by id,date_trunc('day',valid_from)
        order by dbt_valid_from desc
      ) = 1 then true else false end as is_last_snapshot_of_day

  
from user_snapshot u
