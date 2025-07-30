WITH reporting_group_engagement_metrics AS (

  select * from {{ source('app', 'reporting_group_engagement_metrics') }}

),

current_reporting_group_engagement_metrics as (

  select
    -- primary key
        id as reporting_group_engagement_metric_id,

    -- foreign keys
        reporting_group_id,
        member_id,

    -- logical data
        status,
        product_access,
        total_sessions,
        track_name,

    -- booleans
        care,
        coaching_circles,
        extended_network,
        foundations,
        hidden,
        on_demand,
        primary_coaching,
        workshops,

    -- timestamps
        created_at,
        ended_at,
        last_engaged_at,
        last_session_at,
        next_session_at,
        started_at,
        updated_at
  from reporting_group_engagement_metrics

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_reporting_group_engagement_metrics as (
/*

  The archived records in this CTE are records that have been
  deleted in source db and lost due to ingestion re-replication.

  A large scale re-replication occured in 2023-06 during the Stitch upgrade
  and the creation of the new landing schema - stitch_app_v2.
  The app_archive tables found with a tag 2023_06 hold the records
  that pertain to the deleted records at that time and reference can be found in
  ../models/staging/app/sources_schema_app.yml file.

  Details of the upgrade process & postmortem can be found in the Confluence doc titled:
  "stitch_app_v2 upgrade | Process Reference Doc"
  https://betterup.atlassian.net/wiki/spaces/DATA/pages/3418750982/stitch+app+v2+upgrade+Process+Reference+Doc

*/

  select
    -- primary key
        id as reporting_group_engagement_metric_id,

    -- foreign keys
        reporting_group_id,
        member_id,

    -- logical data
        status,
        product_access,
        total_sessions,
        track_name,

    -- booleans
        care,
        coaching_circles,
        extended_network,
        foundations,
        hidden,
        on_demand,
        primary_coaching,
        workshops,

    -- timestamps
        created_at,
        ended_at,
        last_engaged_at,
        last_session_at,
        next_session_at,
        started_at,
        updated_at
  from {{ ref('base_app__reporting_group_engagement_metrics_historical') }}
)


select * from archived_reporting_group_engagement_metrics
union
{% endif -%}
select * from current_reporting_group_engagement_metrics
