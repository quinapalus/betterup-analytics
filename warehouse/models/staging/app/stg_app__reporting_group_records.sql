{{
  config(
    tags=['classification.c2_restricted']
  )
}}

WITH reporting_group_records AS (

  select * from {{ source('app', 'reporting_group_records') }}

),

destroyed_records AS (

  SELECT * FROM {{ref('stg_app__versions_delete')}}
  WHERE item_type = 'ReportingGroupRecord'

),

current_reporting_group_records as (

  SELECT
    id AS reporting_group_record_id,
    reporting_group_id,
    associated_record_type,
    associated_record_id,
    {{ load_timestamp('created_at') }},
    {{ load_timestamp('updated_at') }}
  FROM reporting_group_records AS rgr
  LEFT JOIN destroyed_records AS v ON rgr.id = v.item_id
  WHERE v.item_id IS NULL

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_reporting_group_records as (
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
    id AS reporting_group_record_id,
    reporting_group_id,
    associated_record_type,
    associated_record_id,
    {{ load_timestamp('created_at') }},
    {{ load_timestamp('updated_at') }}
  from {{ ref('base_app__reporting_group_records_historical') }}
)


select * from archived_reporting_group_records
union
{% endif -%}
select * from current_reporting_group_records
