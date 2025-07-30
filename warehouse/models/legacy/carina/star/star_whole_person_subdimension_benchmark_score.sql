{{
  config(
    tags=['eu']
  )
}}

with source as (
SELECT
{{ dbt_utils.star(from=ref('fact_whole_person_subdimension_benchmark_score'), relation_alias='fs') }},
{{ dbt_utils.star(from=ref('dim_whole_person_subdimension'), except=["WHOLE_PERSON_SUBDIMENSION_KEY"], relation_alias='ds') }}
FROM {{ref('fact_whole_person_subdimension_benchmark_score')}} AS fs
INNER JOIN {{ref('dim_whole_person_subdimension')}} AS ds
  ON fs.whole_person_subdimension_key = ds.whole_person_subdimension_key
),

final as (
  select
  {{ dbt_utils.surrogate_key(['whole_person_model_version', 'whole_person_subdimension_key', 'industry', 'employee_level'])}} as _unique,
    *
  from source
)

select * from final