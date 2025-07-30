with previous_day_amplitude_events as (
    select * from {{ ref('stg_amplitude__events')}}
/*Amplitude, the vendor, runs a deduplication script that is sometimes 24 hours 
behind the duplicated data, which results in our tests failing and our dimensional models to 
render incorrect data. Despite their efforts to fix this, we introduce a date filter on this intermediate table
file to pull data at least 24 hours from the current date.
*/
    where true
        and datediff('hour', event_time, current_date) >= 24
    /*ensures betterup email addresses are removed from downstream reports.
    Staging layer file contains a coalesce to set null values for email to none_available, which
    prevents this filter from ignoring nulls.
    */ 
        and email not like '%betterup.co%'
),

final as (
    select 
        *
    from previous_day_amplitude_events
)

select * from final