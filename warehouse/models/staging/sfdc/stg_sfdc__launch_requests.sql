select
    --ids
    id as sfdc_launch_request_id,
    opportunity_c as sfdc_opportunity_id,
    created_by_id as launch_request_created_by_id,
    deployment_manager_c as deployment_manager_id,
    record_type_id as sfdc_record_type_id,

    --categorical and text attributes
    name as launch_request_name,
    status_c as status,
    complexity_c as complexity, 

    --quantities
        --hours
    dm_customer_interaction_hrs_spent_c as deployment_manager_customer_interaction_hours,
    dm_internal_coordination_hrs_spent_c as deployment_manager_internal_coordination_hours,
    dm_technical_configurations_hrs_spent_c as deployment_manager_technical_configuration_hours,
    dm_member_orientation_prep_hours_c as  deployment_manager_member_orientation_prep_hours,
        --licenses
    specialist_on_demand_circles_c as specialist_on_demand_circles_licenses,
    executive_specialist_on_demand_circles_c as executive_specialist_on_demand_circles_licenses,
    coaching_circles_within_company_c as coaching_circles_within_company_licenses,
    connect_beta_c as connect_beta_licenses,
    lead_specialist_on_demand_c as lead_specialist_on_demand_licenses,
    lead_executive_1_1_specialist_od_c as lead_executive_one_to_one_on_demand_licenses,
    care_c as care_licenses,
    x1_1_coaching_only_c as one_to_one_coaching_only_licenses,
    coaching_circles_multi_company_c as coaching_circles_multi_company_licenses,
    lead_sales_performance_c as lead_sales_performance_licenses,
    
    --booleans
    {{ convert_yes_no_string_to_boolean('self_serve_diy_deployment_c') }} as is_self_serve_diy_deployment,

    --dates and timestamps
    {{ load_timestamp('created_date', alias='created_at') }},
    {{ load_timestamp('proposed_launch_date_c', alias='proposed_launch_date') }},

    --other
    is_deleted

from {{ source('salesforce', 'launch_requests') }}
