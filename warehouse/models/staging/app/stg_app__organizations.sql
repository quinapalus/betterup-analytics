{{
  config(
    tags=['eu']
  )
}}

with source as (
    select * from {{ source('app', 'organizations') }}
),

renamed as (

    select
        id AS organization_id,
        uuid AS organization_uuid,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }},
        name,
        salesforce_account_identifier AS sfdc_account_id,
        terms_of_service,
        risk_tier,
        account_segment,
        email_domain_list,
--        product_subscriptions_enabled, --this column does not exist anymore in the postgres DB
        product_subscriptions_enabled_at,
        admin_access_allowed,
        has_labs_data,
        features,
        messaging_restricted,
        default_base_experience_track_id,
        users_account_change_notifications_default,
        reference_population_id,
        disable_phone_number,
        partner_integration_settings_enabled,
        v2_psa_enabled_at

    from source

)

select * from renamed
