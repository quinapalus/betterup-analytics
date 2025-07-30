WITH product_subscription_assignment_migration_audits AS (

  select * from {{ source('app', 'product_subscription_assignment_migration_audits') }}

),

current_product_subscription_assignment_migration_audits as (

  select
  id AS migration_audits_id,
  organization_id,
  user_id,
  contract_line_item_id,
  -- v1 fields
  product_subscription_assignment_v1_id,
  product_subscription_assignment_v1_product_id,
  {{ load_timestamp('product_subscription_assignment_v1_starts_at') }},
  {{ load_timestamp('product_subscription_assignment_v1_ends_at') }},
  {{ load_timestamp('product_subscription_assignment_v1_ended_at') }},
  -- v2 fields
  product_subscription_assignment_v2_id,
  product_subscription_assignment_v2_product_id,
  {{ load_timestamp('product_subscription_assignment_v2_starts_at') }},
  {{ load_timestamp('product_subscription_assignment_v2_ends_at') }},
  {{ load_timestamp('product_subscription_assignment_v2_ended_at') }},
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
  from product_subscription_assignment_migration_audits

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_product_subscription_assignment_migration_audits as (
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
  id AS migration_audits_id,
  organization_id,
  user_id,
  contract_line_item_id,
  -- v1 fields
  product_subscription_assignment_v1_id,
  product_subscription_assignment_v1_product_id,
  {{ load_timestamp('product_subscription_assignment_v1_starts_at') }},
  {{ load_timestamp('product_subscription_assignment_v1_ends_at') }},
  {{ load_timestamp('product_subscription_assignment_v1_ended_at') }},
  -- v2 fields
  product_subscription_assignment_v2_id,
  product_subscription_assignment_v2_product_id,
  {{ load_timestamp('product_subscription_assignment_v2_starts_at') }},
  {{ load_timestamp('product_subscription_assignment_v2_ends_at') }},
  {{ load_timestamp('product_subscription_assignment_v2_ended_at') }},
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
  from {{ ref('base_app__product_subscription_assignment_migration_audits_historical') }}
)


select * from archived_product_subscription_assignment_migration_audits
union
{% endif -%}
select * from current_product_subscription_assignment_migration_audits
