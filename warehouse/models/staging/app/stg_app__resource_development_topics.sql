{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH resource_development_topics AS (

  select * from {{ source('app', 'resource_development_topics') }}

),

current_resource_development_topics as (

  select
      id AS resource_development_topic_id,
      resource_id,
      development_topic_id,
      {{ load_timestamp('created_at') }},
      {{ load_timestamp('updated_at') }}
  from resource_development_topics

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_resource_development_topics as (
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
      id AS resource_development_topic_id,
      resource_id,
      development_topic_id,
      {{ load_timestamp('created_at') }},
      {{ load_timestamp('updated_at') }}
  from {{ ref('base_app__resource_development_topics_historical') }}
)


select * from archived_resource_development_topics
union
{% endif -%}
select * from current_resource_development_topics
