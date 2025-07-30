with source as (
    select * from {{ source('amplitude', 'events') }}
),

renamed as (
    select
    --primary key
        uuid,

    --foreign keys
        user_id,
        adid,
        amplitude_attribution_ids,
        amplitude_id,
        event_id,
        session_id,
        device_id,

    --attributes
        --event attributes
        event_properties,
        event_time,
        event_type,
        amplitude_event_type,

        --timestamps
        client_event_time,
        client_upload_time,
        server_received_time,
        server_upload_time,
        processed_time,

        --device  & location details
        device_brand,
        device_carrier,
        device_family,
        device_manufacturer,
        device_model,
        device_type,
        location_lat,
        location_lng,
        os_name,
        os_version,
        ip_address,
        city,
        country,
        platform,
        region,

        --user details
        user_creation_time,
        user_properties,
        coalesce(user_properties['$email']::string, 'none_available') as email,
        coalesce(user_properties['track_deployment_type']::string, 'none_available') as track_deployment_type,
        
        --version details
        app,
        start_version,
        version_name,

        --misc
        data,
        dma,
        followed_an_identify,
        group_properties,
        groups,
        idfa,
        is_attribution_event,
        language,
        library,
        paying,
        sample_rate

    from source
)

select * from renamed


