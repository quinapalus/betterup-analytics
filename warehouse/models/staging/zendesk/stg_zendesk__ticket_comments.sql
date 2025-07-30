with source as (

    select * from {{ source('zendesk', 'ticket_comments') }}

),

renamed as (

    select
        id as ticket_comment_id,
        created_at,
        received_at,
        author_id,
        ticket_id,
        ticket_event_id,
        type,
        via,
        body,
        public,
        uuid_ts,
        metadata_custom_sdk_device_battery,
        metadata_flags_options_21_trusted,
        metadata_system_ip_address,
        metadata_custom_sdk_device_os,
        metadata_flags_options_15_trusted,
        metadata_suspension_type_id,
        metadata_system_longitude,
        metadata_custom_sdk_device_storage,
        metadata_custom_sdk_device_total_memory,
        metadata_system_eml_redacted,
        metadata_system_message_id,
        metadata_flags_0,
        metadata_system_location,
        metadata_system_email_id,
        metadata_system_raw_email_identifier,
        metadata_trusted,
        metadata_custom_sdk_device_model,
        metadata_system_json_email_identifier,
        metadata_system_latitude,
        metadata_custom,
        metadata_system_client,
        metadata_flags_options_3_trusted,
        metadata_flags_options_2_trusted,
        metadata_flags_1,
        metadata_custom_sdk_device_name,
        metadata_custom_sdk_device_used_memory,
        metadata_custom_sdk_device_manufacturer,
        metadata_custom_sdk_device_low_memory,
        metadata_custom_sdk_device_api,
        metadata_flags_options_9_trusted,
        metadata_notifications_suppressed_for_0,
        metadata_custom_created_via,
        metadata_flags_2,
        metadata_flags_options_25_trusted,
        metadata_custom_who_5

    from source

)

select * from renamed
