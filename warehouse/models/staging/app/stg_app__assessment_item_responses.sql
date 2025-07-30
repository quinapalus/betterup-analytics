WITH assessment_item_responses AS (

  select * from {{ source('app', 'assessment_item_responses') }}

),

current_assessment_item_responses as (

  select
        id as assessment_item_response_id,
        assessment_id,
        assessment_item_id,
        assessment_item_uuid,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}
  from assessment_item_responses

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_assessment_item_responses as (
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
        id as assessment_item_response_id,
        assessment_id,
        assessment_item_id,
        assessment_item_uuid,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}
  from {{ ref('base_app__assessment_item_responses_historical') }}
)


select * from archived_assessment_item_responses
union
{% endif -%}
select * from current_assessment_item_responses



