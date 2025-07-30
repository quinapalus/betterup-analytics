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

renamed as (
    select 
    -- primary key
    /*This ensures for each assignment event, we get the uuid for when the user was assigned to a experiment_name and experirment_variant_group.
    */
        {{ dbt_utils.surrogate_key(['uuid', 'experiment_assignment_key_index_0', 'experiment_assignment_value'])}} as id,

        --foreign keys
        uuid,
        user_id,
        {{ dbt_utils.surrogate_key(['experiment_assignment_key_index_0'])}} as experiment_name_sk,
        {{ dbt_utils.surrogate_key(['experiment_assignment_value'])}} as experiment_variant_name_sk,

        --event details
        event_type,
        event_time,

        --experiment details
        experiment_assignment_key_index_0 as experiment_name,
        experiment_assignment_value as experiment_variant_name
        
    from experiment_assignment_flat
)

select * from renamed
