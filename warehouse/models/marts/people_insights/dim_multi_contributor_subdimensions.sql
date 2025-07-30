
with bu_multi_contributor_subdimensions as (
    select * from {{ ref('bu_multi_contributor_subdimensions') }}
),
final as (
    select {{ dbt_utils.surrogate_key(['whole_person_model_version','assessment_subdimension_key']) }} as primary_key,
        assessment_subdimension_key,
        subdimension_name,
        whole_person_model_version
    from bu_multi_contributor_subdimensions
)
select * from final
