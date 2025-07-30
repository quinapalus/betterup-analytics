with experiment_exposures as (
    select * from {{ ref('int_amplitude__events')}}
    where true
        and event_type = '[Experiment] Exposure'
),

final as (
select 
        *,
  
        --exposure events ensure a user in a given experiment saw the change. 
        event_properties['[Experiment] Flag Key']::string as experiment_name,
        event_properties['[Experiment] Variant']::string as experiment_variant_name,

        --create surrogate keys to make it easier to join to downstream tables
        {{ dbt_utils.surrogate_key(['experiment_name'])}} as experiment_name_sk,
        {{ dbt_utils.surrogate_key(['experiment_variant_name'])}} as experiment_variant_name_sk,

        -- custom columns
        true as is_exposed,
        event_time as change_exposed_at

from experiment_exposures
)


select * from final