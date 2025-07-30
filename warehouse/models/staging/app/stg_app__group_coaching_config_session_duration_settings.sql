with source as (

    select * from {{ source('app', 'group_coaching_config_session_duration_settings') }}

),

renamed as (

    select
        id as group_coaching_config_session_duration_setting_id,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }},
        duration_minutes,
        payment_duration_minutes,
        session_duration_configurable_id,
        session_duration_configurable_type
    from source

)

select * from renamed

