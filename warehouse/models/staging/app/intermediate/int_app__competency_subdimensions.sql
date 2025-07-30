with competency_mappings as (

    select * from {{ ref('stg_app__competency_mappings') }}

)

, item_mappings as (

    select * from {{ ref('stg_app__item_mappings') }}

)

 , final as (

    select
        {{ dbt_utils.surrogate_key(['competency_mappings.competency_mapping_id', 'item_mappings.item_mapping_id', 'internal_names.value::string']) }}
            as competency_subdimension_id,
        competency_mappings.competency_mapping_id,
        competency_mappings.competency_mapping_name as competency_framework,
        competency_mappings.created_at as competency_framework_created_at,
        competency_mappings.organization_id,
        item_mappings.item_mapping_id as competency_id,
        item_mappings.external_name as competency_name,
        item_mappings.description as competency_description,
        item_mappings.external_tag as competency_key,
        item_mappings.internal_names as wpm_subdimensions,
        internal_names.value::string as competency_subdimension_key
    from item_mappings
    inner join competency_mappings
        on item_mappings.competency_mapping_id = competency_mappings.competency_mapping_id
    join lateral flatten (input => item_mappings.internal_names) as internal_names
    where item_mappings.mapping_type = 'wpm_subdimension'

)

 select * from final 
