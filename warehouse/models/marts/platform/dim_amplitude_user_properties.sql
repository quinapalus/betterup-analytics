{{
  config(
    materialized='incremental'
    , unique_key='primary_key'
    , merge_update_columns=['user_id','property_name']
  )
}}
/*This incremental config may look a bit strange. This should run a "merge"
 statement in snowflake joining on the unique_key(s) defined above. However, I really 
 only want new records. Therefore I set the merge_update_columns to update the same keys.
 The property_value and the event_time will not get updated. This is unique b/c we
 want the oldest record, when typically we want the newest. - Tanner C.*/

with amplitude_events as (
    select *
    from {{ ref('int_amplitude__events') }}
),

user_properties as (
    select user_id, 
    user_properties,
    event_time
    from amplitude_events
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        --  subtracting 1 day from max event_time to catch any data out of order
        where event_time > (select dateadd(day, -1, max(event_time)) from {{ this }})
    {% endif %}
)

, user_properties_flattened as
(
    select try_to_number(user_id) as user_id,
        event_time,
        t.key as property_name,
        t.value::string as property_value
    from user_properties
    ,LATERAL FLATTEN(input => USER_PROPERTIES) t
    where try_to_number(user_id) is not null --exclude user records that don't have an int user_id
) 

, experiments as
(
    select user_id,
    event_time,
    property_name,
    property_value,
    row_number() over(partition by user_id, property_name order by event_time asc) as user_property_sequence_number
    from user_properties_flattened
    where property_name like '[Experiment]%'
)

, final as 
(
    select {{ dbt_utils.surrogate_key(['user_id', 'property_name']) }} as primary_key,
    user_id,
    event_time,
    property_name,
    property_value
    from experiments
    where user_property_sequence_number = 1 --we only want the oldest value per user and property_name(experiment)
)

select *
from final

