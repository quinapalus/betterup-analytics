with experiment_assignment as (
    select * from {{ ref('int_amplitude__experiment_assignment_events')}}
),

experiment_assignment_flat as (
    select 
        experiment_assignment.*,
        value.key as experiment_assignment_key,
    
        --experiment assignment split
        split(value.key::string, '.')::variant as experiment_assignment_key_split_array,
        split(value.key::string, '.')[0]::string as experiment_assignment_key_index_0,
        split(value.key::string, '.')[1]::string as experiment_assignment_key_index_1,

        value.value::string as experiment_assignment_value,
        value.this as experiment_assignment_json
    
    from experiment_assignment, 
        lateral flatten(input => event_properties) as value
),

final as (
    select
        --primary key
        /* This key is added to ensure testing after flatttening of the experiment assignment events 
        is unique per assignment_key and assignment_value. */ -- Adrian Lievano

        {{ dbt_utils.surrogate_key(['uuid', 'experiment_assignment_key', 'experiment_assignment_value']) }} as id,

        --foreign keys
        uuid,
        user_id,

        --attributes
        /* Each experiment property JSON for an amplitude experiment assignment event contains all active experiment assignment groups
        for the user at the timestamp of the event in a record. This table is flattened on the JSON object, event_properties. 
        experiment assignment key contains fields for the Environment Name, the {experiment_name}.{variant_name}, the Environment ID provided, etc.
        This table contains all B2B, B2C experiments as well. */ -- Adrian Lievano
        event_type,
        experiment_assignment_key,
        experiment_assignment_key_split_array,
        experiment_assignment_key_index_0,
        experiment_assignment_key_index_1,

        experiment_assignment_value,

        --timestamps
        event_time,

        --metadata
        event_properties

    from experiment_assignment_flat
)

select * from final