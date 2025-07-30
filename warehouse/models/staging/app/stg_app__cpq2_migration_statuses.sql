with source as (

    select * from {{ source('app', 'cpq2_migration_statuses') }}

),

renamed as (

    select
        id as cpq2_migration_status_id,
        user_id,
        job_report_id, 
        organization_id, 
        color, 
        errored_jobs, 
        finished_jobs, 
        status, 
        status_message, 
        status_type, 
        total_jobs, 
        {{ load_timestamp('confirmed_at') }},
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}

    from source

)

select * from renamed