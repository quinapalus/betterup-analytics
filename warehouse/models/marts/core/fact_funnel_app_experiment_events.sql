{{
    config(
        materialized='incremental',
        unique_key='id',
        on_schema_change='sync_all_columns'
    )
}}

with subset_events as (
    select * from {{ ref('int_amplitude__events') }}
    where true
        and event_type in (
        /* The events in this fact table are limited to only include events
        that correspond to core funnel progression steps. Separate fact tables 
        will be created to track specific events to avoid creating an unnecessary large 
        table for DBT jobs to build.
        */
            'Subscription Created',
            'Account Creation Completed',
            'Appointment Created',
            'Appointment Confirmed',
            'Appointment Completed',
            'First Session Scheduled',
            'Coach recommendations requested',
            'Coaches Recommended',
            'User Created',
            'User Profile Completed'
        )
    
),

/*Flatten the subset events table to obtain user properties asssociated with these events.
Then, filter where the key of flattened event for only experiment user properties, so we can 
attribute an experiment_name, experiment_variant_name, and user_id to an event they take. 
*/
flattened as (
    select 
        subset_events.*,
        --these columns here are for future debugging purposes
        flat_records.key::string as key,
        flat_records.value::string as value,

        --extract experiment related attributes from the flattened user properties
        split(flat_records.key, ' ')[1]::string as experiment_name,
        flat_records.value::string as experiment_variant_name

    from subset_events,
    lateral flatten(input => user_properties) as flat_records
    where true 
    /*creates a record for each experiment that a user is part of when the event takes place
    */
        and flat_records.key like '%[Experiment]%'
),

incremental_model as (
    select 
        *
    from flattened
    /* Adding incremental model logic decreases subsequent runtime of this fact table from 222 seconds to 17 seconds.
    */
    {% if is_incremental() %}
  -- this filter will only be applied on an incremental run
     where event_time >= (select max(event_time) from {{ this }})
    {% endif %}
),

final as (
    select 
        --primary key
        /*
        Each user can have multiple experiment assignments during an amplitude event.
        This surrogate key ensures the granularity of table is unique.
        */
        {{ dbt_utils.surrogate_key(['uuid', 'user_id', 'experiment_name', 'experiment_variant_name'])}} as id,

        --foreign key
        uuid,
        user_id,

        --attributes
        event_time,
        event_type,
        email,
        track_deployment_type,
        experiment_name,
        experiment_variant_name
    from incremental_model
)

select * from final