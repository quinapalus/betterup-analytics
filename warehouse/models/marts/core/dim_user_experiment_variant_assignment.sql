with experiment_assignment_flat as (
    select * from {{ ref('int_amplitude__flattened_experiment_assignment_events')}}
    where true 
    /* Captures the variant assignment and details of an experiment assignment. This conditional added filters experiment assignment events
       where the endpoint value, environment id, and environment name configurations. All experiments, across B2B and B2C are 
       captured by only grabbing 'details' and 'variant' information for an experiment assignment event. Because experiments are not
       mutually exclusive, a user can simultaneously be part of multiple experiments at the same time. During an 
       experiment assignment, the event_properties JSON object will contain concurrent experiment assignments that 
       are active. Therefore, a MIN() and MAX() aggregation on the variant assignment value is used to grab the first_assignment_at
       and last_assignment_at timestamps for each user_id in a variant group within an experiment.  
    */
        and experiment_assignment_key_index_1 = 'variant'
),

experiment_exposures as (
    select * from {{ ref('int_amplitude__experiment_exposure_events')}}
),

agg_experiment_exposures as (
    select 
        user_id,
        experiment_name_sk,
        experiment_variant_name_sk,

        /* A user can be exposed to an experiment change multiple times. We attribute all the events a 
        user completes after they first experience the experiment change. 
        */
        min(change_exposed_at) as change_first_exposed_at,
        count(distinct uuid) as total_exposure_events

    from experiment_exposures
    group by 
        user_id,
        experiment_name_sk,
        experiment_variant_name_sk
),

agg_experiment_assignments as (
    select 
        --foreign keys
        user_id,
        {{ dbt_utils.surrogate_key(['experiment_assignment_key_index_0'])}} as experiment_name_sk,
        {{ dbt_utils.surrogate_key(['experiment_assignment_value'])}} as experiment_variant_name_sk,

        --event details
        event_type,

        --experiment details
        experiment_assignment_key_index_0 as experiment_name,
        experiment_assignment_value as experiment_variant_name,

        --aggregations
        count(*) as total_event_assignments,
        min(event_time) as first_assignment_at,
        max(event_time) as last_assignment_at
    from experiment_assignment_flat
    group by 
        user_id,
        experiment_name_sk,
        experiment_variant_name_sk,
        event_type,
        experiment_name,
        experiment_variant_name
),

joined as (
    select 
        agg_experiment_assignments.*,
        --shows when the actual user witnessed the change.
        agg_experiment_exposures.change_first_exposed_at,
        agg_experiment_exposures.total_exposure_events
    from agg_experiment_assignments
    left join agg_experiment_exposures
    /*This joins allows us to join the FLAG that determines is a given user_id assigned to an experiment and experiment variant
    ever seees the change that is part of the experiment. This JOIN with multiple AND statements ensures that the user_id in the same 
    experiment and same variant group is aligned to the assignment event.
    */
        on agg_experiment_assignments.user_id = agg_experiment_exposures.user_id
        and agg_experiment_assignments.experiment_name_sk = agg_experiment_exposures.experiment_name_sk
        and agg_experiment_assignments.experiment_variant_name_sk = agg_experiment_exposures.experiment_variant_name_sk
),

final as (
    select 
        --primary key
        {{ dbt_utils.surrogate_key(['user_id', 'experiment_name', 'experiment_variant_name']) }} as id,

        --foreign key
        user_id,

        --attributes
        experiment_name,
        experiment_variant_name,
        coalesce(total_exposure_events, 0) as total_exposure_events,
        iff(coalesce(total_exposure_events, 0) > 0, true, false) as is_exposed,

        --timestamps
        first_assignment_at,
        last_assignment_at,
        change_first_exposed_at
from joined
)

select * from final