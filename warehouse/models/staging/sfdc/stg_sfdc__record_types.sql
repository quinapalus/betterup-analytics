select
    --ids
    id as sfdc_record_type_id,
    created_by_id as record_type_created_by_id,

    --categorical and text attributes
    name as record_type_name,
    developer_name as record_type_developer_name,
    sobject_type,
    namespace_prefix,

    --boooleans
    is_active as is_active_record_type


from {% if env_var('DEPLOYMENT_ENVIRONMENT', '') == 'US Gov' %} 
            {{ ref('base_fed_sfdc__record_types') }} 
        {% else %} 
            {{ source('salesforce', 'record_types') }}
        {% endif %}





