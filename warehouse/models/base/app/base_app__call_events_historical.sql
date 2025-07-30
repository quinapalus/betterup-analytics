with archived_call_events as (

    select
        call_id,
        contact_method::varchar as contact_method,
        {{ load_timestamp('created_at') }},
        data::varchar as data,
        event_name::varchar as event_name,
        id,
        {{ load_timestamp('updated_at') }},
        user_id
    from {{ source('app_archive', 'call_events') }}

)

select * from archived_call_events
