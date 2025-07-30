with competency_mappings as (

  select * from {{ source('app', 'competency_mappings') }}

)

, final as (

    select
        id as competency_mapping_id,
        organization_id,
        trim(name) as competency_mapping_name,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}
    from competency_mappings

)

select * from final
