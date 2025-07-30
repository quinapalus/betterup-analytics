with whole_person_subdimensions as (

  select * from {{ source('assessment', 'whole_person_subdimensions') }}

),

renamed as (

    select
        {{ dbt_utils.surrogate_key(['whole_person_model_version', 'key']) }} as primary_key,
        whole_person_model_version,
        construct_id,
        key as subdimension_key,
        name as name,
        category,
        {{ snake_case('category') }} as category_key,
        domain,
        {{ snake_case('domain') }} as domain_key,
        dimension,
        {{ snake_case('dimension') }} as dimension_key,
        visible_to_partner

    from whole_person_subdimensions

)

select * from renamed
