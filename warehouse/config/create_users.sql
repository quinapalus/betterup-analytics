create user stitch_user
    password = '<generate this>'
    default_warehouse = stitch_loading -- or 'stitch_loading'
    default_role = stitch_loader; -- or 'stitch_loader'

create user segment_user
    password = '<generate this>'
    default_warehouse = segment_loading
    default_role = segment_loader;

create user tap_user
    password = '<generate this>'
    default_warehouse = tap_loading
    default_role = tap_loader;

create user kafka_user
    password = '<generate this>'
    default_warehouse = tap_loading
    default_role = kafka_loader;

create user dbt_cloud_user
    password = '<generate this>'
    default_warehouse = transforming
    default_role = transformer;

create user mode_user
    password = '<generate this>'
    default_warehouse = reporting
    default_role = reporter;

create user pad_labs_user
    password = '<shared via lastpass>'
    default_warehouse = app_reporting
    default_role = looker_role;

create user catalyst_service_user -- Catalyst https://betterup.atlassian.net/browse/INFOSEC-663
    password = '<generate this'
    default_warehouse = reporting
    default_role = catalyst_service;

-- Data Action Server service users https://betterup.atlassian.net/browse/BUAPP-14402
create user data_action_service_user
    password = '<shared via lastpass>'
    default_warehouse = reporting
    default_role = data_action_service;

create user data_action_service_user_test
    password = '<shared via lastpass>'
    default_warehouse = reporting
    default_role = data_action_service_test;

create user data_action_service_user_dev
    password = '<shared via lastpass>'
    default_warehouse = reporting
    default_role = data_action_service_dev;

/* iterate the following two as needed */
create user jeremy_fishtown
    password = '_generate_this_'
    default_warehouse = transforming
    default_role = transformer;

create user analyst_name_client_name    -- e.g. ken_nestio
    password = '_generate_this_'    -- and send to them securely (e.g. 1ty.me)
    default_warehouse = transforming
    default_role = transformer
    must_change_password = true;

create user salesforce_service_user
    password = '<generate this>'
    default_warehouse = reporting
    default_role = salesforce_service;

create user identify_user
    password = '<generate this>'
    default_warehouse = identify_loading
    default_role = identify_loader;

create user feast_user
    password = '<generate this>'
    default_warehouse = identify_loading
    default_role = feast_loader;

create user resource_metadata_lambda_snowflake_user
    password = '<generate this>'
    default_warehouse = identify_loading
    default_role = resource_metadata_loader;

create user betterup_app_user
    password = '<generate this>'
    default_warehouse = app_reporting
    default_role = betterup_app_role;

create user betterup_app_active_admin_user
    password = '<generate this>'
    default_warehouse = reporting
    default_role = betterup_app_active_admin_role;

create user labs_user
    password = '<generate this>'
    default_warehouse = identify_loading
    default_role = identify_loader
    must_change_password = true;

-- Create Datadog user
create user snowflake_monitor_user
    password = '<generate this>'
    default_warehouse = reporting
    default_role = snowflake_monitor
    default_namespace = SNOWFLAKE.ACCOUNT_USAGE;

-- Create Monte Carlo's user 
create user monte_carlo_user
    password = '<generate this>'
    default_warehouse = monte_carlo_wh
    default_role = monte_carlo_role;
    
create user p2pc_user
    default_warehouse = p2pc_wh
    rsa_public_key = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEArVfLHmurwNAS8sL8Fqqz
Qvt/VNuHGusS5F40G4fyFJs3m8rU3/i56/49d3s92K7kuQr+qDW0SrXUq+BR9mYs
pICFb3W11LFwdsecJJK0ENqofgYEqe382T2Z27m5zq4PVBgBsZeMTfWgSOnsLn4g
ynRb0E5xSAtcOtGH70pHWIwoVtnjSjxU8MiZR/K04S9Bug8ptYTtnf/KEpmWF3NU
QB+8cQgqFEypGBfYIw/cfmXNdyAuCDZPxsuD8DJE1ilDsc2oK5w5I4G41kWeM13u
+LU3Pwo8bD2G4ynyc4PaoTOoMJ99N0kdXRi3yuz5sAoxH6pO3M+oRpo8zQ17dos/
pwIDAQAB'
    default_role = SNOWPIPE_REST_CALLER;


create user BU_INSIGHTS
    default_role = BU_INSIGHTS
    default_warehouse = REPORTING;
