{{
  config(
    tags=['eu']
  )
}}

with source as (
    select * from {{ source('app', 'contracts') }}
),
destroyed_records as (
    select *
    from {{ ref('stg_app__versions_delete')}}
    where item_type = 'Contract'

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
        id as contract_id,

        --Foreign Keys
        organization_id,
        salesforce_contract_id,
        salesforce_opportunity_identifiers AS sfdc_opportunity_ids,

        --Logical data
        name,
        salesforce_contract_number,
        trial,
        --Timestamps
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}

    from filtered

)
{%- if env_var('DEPLOYMENT_ENVIRONMENT', '') != 'US Gov' %},

archived_contracts as (
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
        id as contract_id,

        --Foreign Keys
        organization_id,
        salesforce_contract_id,
        salesforce_opportunity_identifiers AS sfdc_opportunity_ids,

        --Logical data
        name,
        salesforce_contract_number,
        trial,
        --Timestamps
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}

  from {{ ref('base_app__contracts_historical') }}
)


select * from archived_contracts
union
{% endif -%}
select * from renamed
