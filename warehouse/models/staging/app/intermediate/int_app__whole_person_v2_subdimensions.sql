with whole_person_subdimensions as (

    select * from {{ ref('stg_assessment__whole_person_subdimensions') }}

),

filtered as (
    
    select 
        whole_person_model_version,
        construct_id,
        subdimension_key,
        name,
        category,
        category_key,
        domain,
        domain_key,
        dimension,
        dimension_key,
        visible_to_partner
        
    from whole_person_subdimensions
    where whole_person_model_version = 'WPM 2.0'

),

final as (
    select
        {{dbt_utils.surrogate_key(['whole_person_model_version', 'construct_id']) }} as _unique,
        *
    from filtered
)

select * from final
