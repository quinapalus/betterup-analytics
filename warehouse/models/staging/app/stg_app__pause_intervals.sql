WITH pause_intervals AS (

  select * from {{ source('app', 'pause_intervals') }}

),

current_pause_intervals as (

  select
        id as pause_interval_id,
        consumer_subscription_id,
        product_subscription_assignment_id,
        resume_scheduled_job_id,
        pause_scheduled_job_id,
        paused_until,
        status,
        requested_early_unpause,
        {{ load_timestamp('paused_at') }},
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}
  from pause_intervals

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_pause_intervals as (
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
        id as pause_interval_id,
        consumer_subscription_id,
        product_subscription_assignment_id,
        resume_scheduled_job_id,
        pause_scheduled_job_id,
        paused_until,
        status,
        requested_early_unpause,
        {{ load_timestamp('paused_at') }},
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}
  from {{ ref('base_app__pause_intervals_historical') }}
)


select * from archived_pause_intervals
union
{% endif -%}
select * from current_pause_intervals
