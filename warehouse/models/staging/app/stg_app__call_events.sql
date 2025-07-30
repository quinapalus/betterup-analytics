{{
  config(
    tags=["eu"]
  )
}}

with current_call_events as (

  select
    uuid as call_event_id,
    {{ load_timestamp('created_at') }},
    {{ load_timestamp('updated_at') }},
    call_id,
    contact_method,
    PARSE_JSON(data) as event_data,
    event_name,
    user_id
  from {{ ref('base_app__call_events') }}

),

archived_call_events as (
/*
  This is labelled archived, but that may be misleading.
  As of 2023-05-16, data is coming in from both P2PC and Stitch.
  Eventually, all events will come via P2PC.

  Originally a copy of pre 3/14 data was cloned into app_archive, however
  current data is still being written to the original table, and the archive
  is not needed.
*/

  select
    id::varchar as call_event_id,
    {{ load_timestamp('created_at') }},
    {{ load_timestamp('updated_at') }},
    call_id,
    contact_method,
    PARSE_JSON(data) as event_data,
    event_name,
    user_id
  from {{ source('app', 'call_events') }}

),


/*

  !!!!IMPORTANT!!!!
  This union may have duplicate rows for particular events.
  When using any aggregates, ensure you are using DISTINCT where appropriate.
  !!!!!!!!!!!!!!!!!

 */

non_historic_union as (
    select * from archived_call_events
    union distinct
    select * from current_call_events
)

  {%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %}
  select
    id::varchar as call_event_id,
    {{ load_timestamp('created_at') }},
    {{ load_timestamp('updated_at') }},
    call_id,
    contact_method,
    PARSE_JSON(data) as event_data,
    event_name,
    user_id
  from {{ ref('base_app__call_events_historical') }}
  where id::varchar not in (select call_event_id from non_historic_union)
  union
  {% endif -%}
  select * from non_historic_union

