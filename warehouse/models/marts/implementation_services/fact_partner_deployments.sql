select
    --ids
    sfdc_launch_request_id as partner_deployment_id,
    sfdc_account_id,
    sfdc_opportunity_id,
    launch_request_created_by_id as partner_deployment_created_by_id,
    deployment_manager_id,
    account_manager_id,
    account_csm_id,
    sfdc_record_type_id,

    --categories
    launch_request_name as partner_deployment_name,
    status,
    complexity,
    record_type_name,

    --dates
    created_at,
    proposed_launch_date,

    --booleans
    is_self_serve_diy_deployment,

    --quantities
    deployment_manager_customer_interaction_hours,
    deployment_manager_internal_coordination_hours,
    deployment_manager_technical_configuration_hours,
    deployment_manager_member_orientation_prep_hours,
    specialist_on_demand_circles_licenses,
    executive_specialist_on_demand_circles_licenses,
    coaching_circles_within_company_licenses,
    connect_beta_licenses,
    lead_specialist_on_demand_licenses,
    lead_executive_one_to_one_on_demand_licenses,
    care_licenses,
    one_to_one_coaching_only_licenses,
    coaching_circles_multi_company_licenses,
    lead_sales_performance_licenses,

    --measures
    {{ dbt_utils.safe_add('specialist_on_demand_circles_licenses', 'executive_specialist_on_demand_circles_licenses',
                        'coaching_circles_within_company_licenses','connect_beta_licenses','care_licenses',
                        'lead_specialist_on_demand_licenses','lead_executive_one_to_one_on_demand_licenses',
                        'one_to_one_coaching_only_licenses','coaching_circles_multi_company_licenses',
                        'lead_sales_performance_licenses') }}
    as total_licenses,

    {{ dbt_utils.safe_add('deployment_manager_customer_interaction_hours', 'deployment_manager_internal_coordination_hours',
                        'deployment_manager_technical_configuration_hours','deployment_manager_member_orientation_prep_hours') }}
    as total_deployment_manager_hours,

    1 as count_of_partner_deployments

from {{ ref('int_sfdc__launch_requests') }}
