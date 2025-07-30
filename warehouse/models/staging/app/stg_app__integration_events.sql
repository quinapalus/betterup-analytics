with source as (

    select * from {{ source('app', 'integration_events') }}

),

renamed as (

    select
        id as integration_event_id,
        organization_id,
        organization_uuid,
        email,
        event_type as type,
        category,
        details,
        dry_run,
        request_uuid,
        whodunnit_jid,
        extra_data,
        source,
        {{ load_timestamp('timestamp') }}
    from source

)

select * from renamed