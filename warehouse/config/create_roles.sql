-- Must run as role `securityadmin`
create role stitch_loader;
create role segment_loader;
create role tap_loader;
create role kafka_loader;

create role transformer;
create role reporter;
create role looker_role;
create role betterup_app_role;
create role betterup_app_active_admin_role;
create role salesforce_service;

-- Create service roles for consumers of warehouse data
create role catalyst_service; -- Catalyst https://betterup.atlassian.net/browse/INFOSEC-663

-- Create roles for https://github.com/betterup/betterup_data_action_server
create role data_action_service; -- Data Action Server https://betterup.atlassian.net/browse/BUAPP-14402
create role data_action_service_test;
create role data_action_service_dev;

-- Create roles for identify and feast and resource-metadata-service
create role identify_loader
create role feast_loader
create role resource_metadata_loader;

--- role for labs user
create role labs_loader

-- Create a new role intended to monitor Snowflake usage.
create role if not exists snowflake_monitor;

-- Create a role for Monte Carlo to use.
create role if not exists monte_carlo_role;

-- Create role for snowpipe caller
create role if not exists snowpipe_rest_caller;