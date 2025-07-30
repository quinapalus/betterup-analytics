grant all on database raw to role stitch_loader;
grant all on database raw to role segment_loader;
grant all on database raw to role tap_loader;
grant all on database raw to role kafka_loader;
grant all on database raw to role identify_loader;
grant all on database raw to role feast_loader;
grant all on database raw to role resource_metadata_loader;

-- Prod read-only user, used for all types of active admin direct queries by internal users
grant usage on database analytics to role betterup_app_active_admin_role;
grant usage on all schemas in database analytics to role betterup_app_active_admin_role;
grant select on all tables in database analytics to role betterup_app_active_admin_role;
grant select on future tables in database analytics to role betterup_app_active_admin_role;

-- Prod read-only user, used for all types of in-app direct queries by external users
grant usage on database analytics to role betterup_app_role;
grant usage on all schemas in database analytics to role betterup_app_role;
grant select on all tables in database analytics to role betterup_app_role;
grant select on future tables in database analytics to role betterup_app_role;


grant usage on database raw to role transformer;
grant usage on all schemas in database raw to role transformer;
grant usage on future schemas in database raw to role transformer;
grant select on all tables in database raw to role transformer;
grant select on future tables in database raw to role transformer;
grant select on all views in database raw to role transformer;
grant select on future views in database raw to role transformer;

grant usage on schema raw.segment_zendesk to role transformer;
grant select on all tables in schema raw.segment_zendesk to role transformer;
grant select on future tables in schema raw.segment_zendesk to role transformer;
grant select on all views in schema raw.segment_zendesk to role transformer;
grant select on future views in schema raw.segment_zendesk to role transformer;

grant all on database analytics to role transformer;
grant all on database stg_analytics to role transformer;
grant all on database staging_performance to role transformer;
grant all on database "STG_ANALYTICS" to role transformer;
grant all on database "DEV_ANALYTICS" to role transformer;

grant usage on database analytics to role reporter;
grant usage on schema analytics.platform to role reporter;
grant select on all tables in schema analytics.platform to role reporter;
grant select on future tables in schema analytics.platform to role reporter;
grant select on all views in schema analytics.platform to role reporter;
grant select on future tables in schema analytics.platform to role reporter;

-- enable services to use analytics database
grant usage on database analytics to role catalyst_service;

-- enable future grants
grant manage grants on account to role transformer;

grant usage on warehouse stitch_loading to role stitch_loader;
grant all on warehouse stitch_loading to role transformer;
grant usage on warehouse tap_loading to role tap_loader;
grant usage on warehouse tap_loading to role kafka_loader;
grant all on warehouse tap_loading to role transformer;
grant usage on warehouse segment_loading to role segment_loader;
grant all on warehouse segment_loading to role transformer;
grant all on warehouse identify_loading to role identify_loader;
grant all on warehouse identify_loading to role transformer;
grant all on warehouse identify_loading to role feast_loader;
grant all on warehouse identify_loading to role resource_metadata_loader;
grant usage on warehouse reporting to role feast_loader;

grant all on warehouse transforming to role transformer;
grant all on warehouse reporting to role reporter;
grant all on warehouse app_reporting to role looker_role;
grant usage on warehouse app_reporting to role betterup_app_role;
grant usage on warehouse reporting to role betterup_app_active_admin_role;

-- enable services to execute queries using reporting warehouse:
grant usage on warehouse reporting to role catalyst_service;


-- Configure roles for https://github.com/betterup/betterup_data_action_server --

-- grant usage on database (environments use different schemas within the same db):
grant usage on database data_action_server to role data_action_service;
grant usage on database data_action_server to role data_action_service_test;
grant usage on database data_action_server to role data_action_service_dev;

-- grant all for the schemas associated with each environment:
grant all on schema data_action_server.production to role data_action_service;
grant all on schema data_action_server.test to role data_action_service_test;
grant all on schema data_action_server.dev to role data_action_service_dev;

-- give dev role full permissions on current and future tables in dev environment
grant all on all tables in schema data_action_server.dev to role data_action_service_dev;
grant all on future tables in schema data_action_server.dev to role data_action_service_dev;

-- grant schema usage and select on current/future tables in production and dev
-- environments to looker_role. This will enable us to query these tables through Looker.
grant usage on schema data_action_server.production to role looker_role;
grant select on all tables in schema data_action_server.production to role looker_role;
grant select on future tables in schema data_action_server.production to role looker_role;

grant usage on schema "ANALYTICS"."BETTERUP_DIAGNOSTIC_EXT_PROD" to role looker_role;
grant select on all tables in schema "ANALYTICS"."BETTERUP_DIAGNOSTIC_EXT_PROD" to role looker_role;
grant select on all views in schema "ANALYTICS"."BETTERUP_DIAGNOSTIC_EXT_PROD" to role looker_role;
grant select on future views in schema "ANALYTICS"."BETTERUP_DIAGNOSTIC_EXT_PROD" to role looker_role;
grant select on future tables in schema "ANALYTICS"."BETTERUP_DIAGNOSTIC_EXT_PROD" to role looker_role;

grant usage on schema data_action_server.dev to role looker_role;
grant select on all tables in schema data_action_server.dev to role looker_role;
grant select on future tables in schema data_action_server.dev to role looker_role;

grant usage on schema raw.identify to role looker_role;
grant select on all tables in schema raw.identify to role looker_role;

grant usage on schema raw.segment_betterup_ios_prod to role looker_role;
grant select on all tables in schema raw.segment_betterup_ios_prod to role looker_role;
grant select on future tables in schema raw.segment_betterup_ios_prod to role looker_role;

grant usage on schema analytics.semaphore to role looker_role;
grant select on all tables in schema analytics.semaphore to role looker_role;
grant select on future tables in schema analytics.semaphore to role looker_role;
grant select on all views in schema analytics.semaphore to role looker_role;
grant select on future views in schema analytics.semaphore to role looker_role;

-- those command give the looker_role read access to all view/tables in
-- the STAGING_APP schema
grant usage on schema "ANALYTICS"."STAGING_APP" to role looker_role;
grant select on all tables in schema "ANALYTICS"."STAGING_APP" to role looker_role;
grant select on all views in schema "ANALYTICS"."STAGING_APP" to role looker_role;
grant select on future views in schema "STAGING_APP" to role looker_role;
grant select on future tables in schema "STAGING_APP" to role looker_role;

-- these are future grants not already given to looker_role as of 2023-03-21
grant select on future tables in schema "ANALYTICS"."CORE" to role looker_role;
grant select on future tables in schema "ANALYTICS"."PEOPLE_INSIGHTS" to role looker_role;
grant select on future tables in schema "ANALYTICS"."MEMBER" to role looker_role;
grant select on future views in schema "ANALYTICS"."CORE" to role looker_role;
grant select on future views in schema "ANALYTICS"."PEOPLE_INSIGHTS" to role looker_role;
grant select on future views in schema "ANALYTICS"."MEMBER" to role looker_role;

grant all on all tables in schema raw.identify to role transformer;
grant all on future tables in schema raw.identify to role transformer;

grant all on schema raw.RESOURCE_METADATA_SERVICE to role resource_metadata_loader;
grant all on all tables in schema raw.RESOURCE_METADATA_SERVICE to role resource_metadata_loader;
grant all on future tables in schema raw.RESOURCE_METADATA_SERVICE to role resource_metadata_loader;

grant all on schema raw.feast to role feast_loader;
grant all on all tables in schema raw.feast to role feast_loader
grant all on future tables in schema raw.feast to role feast_loader

grant all on schema raw.feast to role transformer;
grant all on all tables in schema raw.feast to role transformer
grant all on future tables in schema raw.feast to role transformer


grant usage on schema raw.segment_hubspot to role transformer;
grant all on all tables in schema raw.segment_hubspot to role transformer;
grant all on future tables in schema raw.segment_hubspot to role transformer;


-- Note: we're not giving the looker_role access to the test environment as we
-- won't need to query these tables in Looker

-- enable data_action_service roles to execute queries using the reporting warehouse:
grant usage on warehouse reporting to role data_action_service;
grant usage on warehouse reporting to role data_action_service_test;
grant usage on warehouse reporting to role data_action_service_dev;

-- grant roles to Data Action Server service users
grant role data_action_service to user data_action_service_user;
grant role data_action_service_test to user data_action_service_user_test;
grant role data_action_service_dev to user data_action_service_user_dev;
-- grant dev role to personal accounts for development in web console:
grant role data_action_service_dev to user ross_feller;
grant role data_action_service_dev to user mshappe;
grant role identify_loader to user identify_user
grant role feast_loader to user feast_user
grant role resource_metadata_loader to user resource_metadata_lambda_snowflake_user;

-- grant roles to users:
grant role stitch_loader to user stitch_user;
grant role segment_loader to user segment_user;
grant role tap_loader to user tap_user;
grant role kafka_loader to user kafka_user;

grant role transformer to user dbt_cloud_user;
grant role transformer to user levi;
grant role transformer to user ross_feller;
grant role reporter to user mode_user;

grant role looker_role to user pad_labs_user;
grant role betterup_app_role to user betterup_app_user;
grant role betterup_app_active_admin_role to user betterup_app_active_admin_user;

-- grant roles to service users:
grant role catalyst_service to user catalyst_service_user;

-- Grant privileges on the SNOWFLAKE database to the new role.
grant imported privileges on database SNOWFLAKE to role snowflake_monitor;

-- Grant snowflake_monitor role to user
grant role snowflake_monitor to user snowflake_monitor_user;

-- Grant permissions to use stg_analytics for role transformer
grant usage on database "STG_ANALYTICS" to role transformer; --this might not be necessary because we already 'grant all on database "STG_ANALYTICS" to role transformer;'
grant usage on all schemas in database "STG_ANALYTICS" to role transformer; --this might not be necessary because we already 'grant all on database "STG_ANALYTICS" to role transformer;'
grant usage on future schemas in database "STG_ANALYTICS" to role transformer; --this might not be necessary because we already 'grant all on database "STG_ANALYTICS" to role transformer;'

grant select on future tables in database "STG_ANALYTICS" to role transformer; --this might not be necessary because we already 'grant all on database "STG_ANALYTICS" to role transformer;'
grant select on future views in database "STG_ANALYTICS" to role transformer; --this might not be necessary because we already 'grant all on database "STG_ANALYTICS" to role transformer;'

grant select on all views in database "STG_ANALYTICS" to role transformer; --this might not be necessary because we already 'grant all on database "STG_ANALYTICS" to role transformer;'
grant select on all tables in database "STG_ANALYTICS" to role transformer; --this might not be necessary because we already 'grant all on database "STG_ANALYTICS" to role transformer;'


--Grant permissions on den_analytics database for looker_role
grant usage on database "DEV_ANALYTICS" to role looker_role;
grant usage on all schemas in database "DEV_ANALYTICS" to role looker_role;
grant usage on future schemas in database "DEV_ANALYTICS" to role looker_role;

grant select on all views in database "DEV_ANALYTICS" to role looker_role;
grant select on all tables in database "DEV_ANALYTICS" to role looker_role;
grant select on future tables in database "DEV_ANALYTICS" to role looker_role;
grant select on future views in database "DEV_ANALYTICS" to role looker_role;

-- Grant Monte Carlo user access to Monte Carlo role
GRANT ROLE monte_carlo_role TO USER monte_carlo_user;

-- Grant permissions to the Monte Carlo role to use the Monte Carlo warehouse
GRANT OPERATE, USAGE, MONITOR ON WAREHOUSE monte_carlo_wh TO ROLE monte_carlo_role;

-- Grant privileges to allow access to query history to Monte Carlo role
GRANT IMPORTED PRIVILEGES ON DATABASE "SNOWFLAKE" TO ROLE monte_carlo_role;

-- grant permissions for stitch_loader for integrations database
grant usage on database integrations to role stitch_loader;

-- Grant the USAGE privilege on the database and schema that contain the pipe object.
grant usage on database raw to role snowpipe_rest_caller;
grant usage on schema raw.p2pc to role snowpipe_rest_caller;

-- Grant the INSERT and SELECT privileges on the target table.
grant insert, select on raw.p2pc.call_events to role snowpipe_rest_caller;

-- Grant the USAGE privilege on the external stage.
grant usage on stage raw.p2pc.CALL_EVENTS_STAGE to role snowpipe_rest_caller;

-- Grant the OPERATE and MONITOR privileges on the pipe object.
grant operate, monitor on pipe raw.p2pc.INGEST_EVENTS to role snowpipe_rest_caller;

-- Grant the role to a user
grant role snowpipe_rest_caller to user p2pc_user;


grant usage on warehouse p2pc_wh to role snowpipe_rest_caller;


GRANT SELECT ON ALL TABLES in SCHEMA RAW.BETTERUP_VIVA_API_STAGING TO ROLE TRANSFORMER;
GRANT SELECT ON FUTURE TABLES in SCHEMA RAW.BETTERUP_VIVA_API_STAGING TO ROLE TRANSFORMER;

GRANT SELECT ON ALL TABLES in SCHEMA RAW.BETTERUP_VIVA_API_PROD TO ROLE TRANSFORMER;
GRANT SELECT ON FUTURE TABLES in SCHEMA RAW.BETTERUP_VIVA_API_PROD TO ROLE TRANSFORMER;

GRANT SELECT ON ALL TABLES in SCHEMA RAW.BETTERUP_VIVA_API_DEV TO ROLE TRANSFORMER;
GRANT SELECT ON FUTURE TABLES in SCHEMA RAW.BETTERUP_VIVA_API_DEV TO ROLE TRANSFORMER;

GRANT ROLE TRANSFORMER TO USER BU_INSIGHTS;