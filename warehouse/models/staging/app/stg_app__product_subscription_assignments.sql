{{
  config(
    tags=["eu"]
  )
}}

WITH product_subscription_assignments AS (

  select * from {{ source('app', 'product_subscription_assignments') }}

),

current_product_subscription_assignments as (

  select
      id AS product_subscription_assignment_id,
      product_subscription_id,
      user_id as member_id,
      stripe_subscription_id,
      stripe_data,
      archived_at,
      parse_json(stripe_data)['customer_id']::string as stripe_customer_id,
      coalesce(v2,False) AS v2,
      {{ load_timestamp('created_at') }},
      {{ load_timestamp('updated_at') }},
      {{ load_timestamp('starts_at') }},
      {{ load_timestamp('ended_at') }},
      {{ load_timestamp('ends_at') }},
      {{ load_timestamp('requested_cancellation_at') }}
  from product_subscription_assignments

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_product_subscription_assignments as (
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
      id AS product_subscription_assignment_id,
      product_subscription_id,
      user_id as member_id,
      stripe_subscription_id,
      stripe_data,
      archived_at,
      parse_json(stripe_data)['customer_id']::string as stripe_customer_id,
      coalesce(v2,False) AS v2,
      {{ load_timestamp('created_at') }},
      {{ load_timestamp('updated_at') }},
      {{ load_timestamp('starts_at') }},
      {{ load_timestamp('ended_at') }},
      {{ load_timestamp('ends_at') }},
      {{ load_timestamp('requested_cancellation_at') }}
  from {{ ref('base_app__product_subscription_assignments_historical') }}
)


select * from archived_product_subscription_assignments
WHERE
  archived_at IS NULL AND
  (DATEDIFF(minute, starts_at, COALESCE(ended_at, ends_at)) > 60 OR
  COALESCE(ended_at, ends_at) IS NULL)
union
{% endif -%}
select * from current_product_subscription_assignments
WHERE
  archived_at IS NULL AND
  (DATEDIFF(minute, starts_at, COALESCE(ended_at, ends_at)) > 60 OR
  COALESCE(ended_at, ends_at) IS NULL)
