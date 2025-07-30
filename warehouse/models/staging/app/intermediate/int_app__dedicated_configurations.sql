{{
  config(
    tags=["eu"]
  )
}}

with dedicated_configurations as (

  select * from {{ ref('stg_app__dedicated_configurations') }}

),

experience_configuration_assignments as (

  select * 
  from {{ ref('stg_app__experience_configuration_assignments') }}
  where configurable_type = 'ExperienceConfigurations::DedicatedConfiguration'

),

final as (
  
  select {{ dbt_utils.surrogate_key(['e.track_id', 'c.dedicated_configuration_id']) }} as int_app__dedicated_configurations_id,
    e.track_id,
    c.* 
  from experience_configuration_assignments as e
  left join dedicated_configurations as c
    on c.dedicated_configuration_id = e.configurable_id
  qualify row_number() over
    (partition by e.track_id order by e.created_at desc) = 1 -- logic that limits to unique record per track; picks up latest config
)

select *
from final
