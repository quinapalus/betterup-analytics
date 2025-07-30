WITH consumer_products AS (

  select * from {{ source('app', 'consumer_products') }}

),

current_consumer_products as (

  select
        --primary key
        id as app_consumer_product_id,

        --foreign keys
        product_id as app_product_id,
        stripe_product_id,

        --attributes
        care as is_care,
        recommended as is_recommended,
        visible as is_visible,
        description,
        name,
        case
            when description like '%1 video session/month%'
                then 1
            when description like '%1 video sessions/month%'
                then 1
            when description like '%2 video sessions/month%'
                then 2
            when description like '%3 video sessions/month%'
                then 3
            when description like '%4 video sessions/month%'
                then 4
            when description like 'Two live video sessions per monthly%'
                then 2
        end as estimated_sessions_purchased,

        ----timestamps
        updated_at,
        created_at

  from consumer_products

)

{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_consumer_products as (
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
        --primary key
        id as app_consumer_product_id,

        --foreign keys
        product_id as app_product_id,
        stripe_product_id,

        --attributes
        care as is_care,
        recommended as is_recommended,
        visible as is_visible,
        description,
        name,
        case
            when description like '%1 video session/month%'
                then 1
            when description like '%1 video sessions/month%'
                then 1
            when description like '%2 video sessions/month%'
                then 2
            when description like '%3 video sessions/month%'
                then 3
            when description like '%4 video sessions/month%'
                then 4
            when description like 'Two live video sessions per monthly%'
                then 2
        end as estimated_sessions_purchased,

        ----timestamps
        updated_at,
        created_at

  from {{ ref('base_app__consumer_products_historical') }}
)


select * from archived_consumer_products
union
{% endif -%}
select * from current_consumer_products
