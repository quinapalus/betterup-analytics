{{
  config(
    materialized='table'
  )
}}

with competency_mappings as (

    select * from {{ ref('stg_app__competency_mappings') }}

)

, item_mappings as (

    select * from {{ ref('stg_app__item_mappings') }}

)

, construct_item_mappings as (

    select * from {{ ref('stg_app__construct_item_mappings') }}

)

, construct_item_mappings_grouped as (

    select construct_uuid, item_mapping_id
    from construct_item_mappings
    group by 1,2

)

, constructs as (

    select * from {{ ref('stg_assessment__constructs') }}

)

 , final as (

    select
        {{ dbt_utils.surrogate_key(['competency_mappings.competency_mapping_id', 'item_mappings.item_mapping_id', 'constructs.construct_key']) }}
            as competency_construct_id,
        competency_mappings.competency_mapping_id,
        item_mappings.item_mapping_id,
        competency_mappings.competency_mapping_name as competency_framework,
        competency_mappings.created_at as competency_framework_created_at,
        competency_mappings.organization_id,
        item_mappings.item_mapping_id as competency_id,
        item_mappings.external_name as competency_name,
        item_mappings.description as competency_description,
        item_mappings.external_tag as competency_key,
        item_mappings.internal_names as wpm_subdimensions,
        constructs.construct_key as competency_construct_key
    from item_mappings
    inner join competency_mappings
        on item_mappings.competency_mapping_id = competency_mappings.competency_mapping_id
    left join construct_item_mappings_grouped
        on item_mappings.item_mapping_id = construct_item_mappings_grouped.item_mapping_id
    left join constructs
        on construct_item_mappings_grouped.construct_uuid = constructs.construct_uuid

)

 select * from final 
