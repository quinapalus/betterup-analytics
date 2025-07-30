with whole_person_subdimensions AS (

  SELECT * FROM {{ ref('int_app__whole_person_v2_subdimensions') }}

)

select
    {{ dbt_utils.surrogate_key(['whole_person_model_version', 'construct_id']) }} as primary_key,
    -- for category = Behavior, use the actual domain value
    -- for category = Mindset/Outcome, fill in the Category value for domain
    case when category = 'Behavior' then domain_key     else category_key   end as domain_key,
    case when category = 'Behavior' then domain         else category       end as domain,
    -- for category = Behavior, use the actual dimension value
    -- for category = Mindset/Outcome, fill in the subdimension value for dimension
    case when category = 'Behavior' then dimension_key  else subdimension_key   end as dimension_key,
    case when category = 'Behavior' then dimension      else name               end as dimension,
    {{ dbt_utils.star(from=ref('int_app__whole_person_v2_subdimensions'), except=["domain", "domain_key", "dimension", "dimension_key"]) }}
from whole_person_subdimensions
