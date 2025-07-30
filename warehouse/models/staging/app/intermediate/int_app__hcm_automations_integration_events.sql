WITH integration_events AS (

  SELECT * FROM {{ ref('stg_app__integration_events') }}

)

SELECT
    {{ dbt_utils.surrogate_key(['organization_id', 'source', 'parse_json(extra_data):"user_attribute_import_id"', 'parse_json(extra_data):"hcm_sync_run_id"']) }} AS primary_key,
    organization_id,
    source,
    parse_json(extra_data):"user_attribute_import_id"  AS "user_attribute_import_id",
    parse_json(extra_data):"hcm_sync_run_id"  AS "hcm_sync_run_id",
    COUNT(DISTINCT CASE WHEN (( category  ) = 'UserProvisioningSucceeded') THEN integration_event_id  ELSE NULL END) AS "count_user_provisioning_succeeded",
    COUNT(DISTINCT CASE WHEN (( category  ) = 'UserProvisioningFailed') THEN integration_event_id  ELSE NULL END) AS "count_user_provisioning_failed",
    COUNT(DISTINCT CASE WHEN (( category  ) = 'UserDeactivationSucceeded') THEN integration_event_id  ELSE NULL END) AS "count_user_deactivation_succeeded",
    COUNT(DISTINCT CASE WHEN (( category  ) = 'UserDeactivationFailed') THEN integration_event_id  ELSE NULL END) AS "count_user_deactivation_failed",
    COUNT(DISTINCT CASE WHEN (( category  ) = 'UserAttributeValueChanged') THEN integration_event_id  ELSE NULL END) AS "count_user_attribute_values_changed",
    MAX(timestamp) AS "max_timestamp",
    MIN(timestamp) AS "min_timestamp",
    MIN(timestamp) AS "timestamp"
FROM
    integration_events
WHERE
    category IN ('UserProvisioningSucceeded','UserProvisioningFailed','UserDeactivationSucceeded','UserDeactivationFailed','UserAttributeImportCreated','UserAttributeImportProcessed','WorkdaySyncWorkersStarted','WorkdaySyncWorkersCompleted','UserAttributeValueChanged')
GROUP BY 
    2, 3, 4, 5