with source as (
      select * from {{ source('segment_frontend', 'assessment_shared_with_coaches') }}
),
renamed as (
    select
        received_at,
        assessment_id,
        timestamp,
        user_id,
        context_user_agent,
        event_text,
        platform,
        user_agent,
        context_locale,
        context_library_name,
        context_page_path,
        id as assessment_shared_with_coaches_id,
        original_timestamp,
        context_amplitude_session_id,
        context_page_title,
        page_url,
        client_features_enabled,
        event,
        page_name,
        sent_at,
        server_features_enabled,
        uuid_ts,
        context_ip,
        context_page_referrer,
        context_page_url,
        using_pwa,
        anonymous_id,
        impersonated_event,
        context_library_version,
        context_page_search,
        context_user_agent_data_mobile,
        context_user_agent_data_brands,
        context_user_agent_data_platform

    from source
)
select * from renamed
  