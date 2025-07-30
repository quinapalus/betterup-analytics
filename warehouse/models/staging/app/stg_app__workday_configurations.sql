with source as (

    select * from {{ source('app', 'workday_configurations') }}

),

renamed as (

    select
        id as workday_configuration_id,
        organization_id,
        integration_system_id,
        rest_api_client_id,
        raas_api_client_id,
        deactivate_on_termination,
        exclude_contingent_workers,
        host,
        tenant,
        username,
        country_value,
        country_key,
        provision_new_users,
        use_work_email_only,
        integration_document_name,
        integration_document_field_name,
        enc_password,
        provision_only_new_hires,
        invitation_flow,
        rest_api_keypair,
        x509_cert,
        enable_outcomes,
        outcomes_report_url,
        raas_x509_cert,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}

    from source

)

select * from renamed