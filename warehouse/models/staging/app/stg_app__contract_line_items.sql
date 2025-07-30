{{
  config(
    tags=['eu']
  )
}}

with source as (
    select * from {{ source('app', 'contract_line_items') }}
),
destroyed_records as (
    select *
    from {{ ref('stg_app__versions_delete')}}
    where item_type = 'ContractLineItem'

),
filtered as (
    select s.*
    from source s
    left join destroyed_records as v
        on s.id = v.item_id
    where v.item_id is null
),
renamed as (

    select
        --Primary Key
        id as contract_line_item_id,

        --Foreign Keys
        contract_id,
        product_id,
        product_subscription_id,
        subscription_terms_id,
        organization_id,
        salesforce_subscription_id,
        salesforce_order_item_identifier,
        salesforce_legacy_root_id,
        salesforce_legacy_subscription_id,

        --Logical data
        seats,
        coaching_months,
        covered_lives,
        deployment_type,
        sku,
        non_transfer_period,

        --Timestamps
        {{ load_timestamp('starts_at') }},
        {{ load_timestamp('ends_at') }},
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}

    from filtered

)
{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_contract_line_items as (
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
        --Primary Key
        id as contract_line_item_id,

        --Foreign Keys
        contract_id,
        product_id,
        product_subscription_id,
        subscription_terms_id,
        organization_id,
        salesforce_subscription_id,
        salesforce_order_item_identifier,
        salesforce_legacy_root_id,
        salesforce_legacy_subscription_id,

        --Logical data
        seats,
        coaching_months,
        covered_lives,
        deployment_type,
        sku,
        non_transfer_period,

        --Timestamps
        {{ load_timestamp('starts_at') }},
        {{ load_timestamp('ends_at') }},
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}

  from {{ ref('base_app__contract_line_items_historical') }}
)


select * from archived_contract_line_items
union
{% endif -%}
select * from renamed

