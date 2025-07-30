with source as (

    select * from {{ source('app', 'hcm_sync_runs') }}

),

renamed as (

    select
        id as hcm_sync_run_id,
        configuration_id,
        hcm_sync_schedule_id,
        workday_configuration_id,
        job_type,
        provision_users,
        allow_user_deactivation,
        allow_user_removal,
        dry_run,
        configuration_type,
        {{ load_timestamp('completed_at') }},
        {{ load_timestamp('started_at') }},
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}

    from source

)

select * from renamed