with source as (

    select * from {{ source('salesforce', 'role') }}

),

renamed as (

    select
        id as role_id,
        received_at,
        case_access_for_account_owner,
        contact_access_for_account_owner,
        developer_name,
        last_modified_by_id,
        {{ environment_varchar_to_timestamp('last_modified_date','last_modified_date') }},
        parent_role_id,
        system_modstamp,
        uuid_ts,
        name,
        opportunity_access_for_account_owner,
        rollup_description,
        forecast_user_id,
        may_forecast_manager_share,
        portal_type

    from source

)

select * from renamed
