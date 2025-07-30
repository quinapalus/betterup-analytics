WITH subdimension_benchmark_scores_reference_population_1 AS (

  SELECT * FROM {{ref('int_app__subdimension_benchmark_scores_reference_population_1')}}

),

dimension_benchmark_scores_reference_population_1 AS (

  SELECT * FROM {{ref('int_app__dimension_benchmark_scores_reference_population_1')}}

),

derived_construct_benchmark_scores_reference_population_1 AS (

  SELECT * FROM {{ref('int_app__derived_construct_benchmark_scores_reference_population_1')}}

),

unioned AS (

  SELECT
    'subdimension' as construct_type,
    whole_person_model_version,
    industry,
    level,
    key,
    mean
  FROM subdimension_benchmark_scores_reference_population_1

  UNION ALL

  SELECT
    'dimension' as construct_type,
    whole_person_model_version,
    industry,
    level as level,
    dimension_key AS key,
    mean
  FROM dimension_benchmark_scores_reference_population_1

  UNION ALL

  SELECT
    'derived_construct' as construct_type,
    whole_person_model_version,
    industry,
    level,
    key,
    mean
  FROM derived_construct_benchmark_scores_reference_population_1

)

SELECT
  {{ dbt_utils.surrogate_key(['construct_type', 'whole_person_model_version', 'industry', 'level', 'key']) }} AS primary_key,
  1 as reference_population_id,
  'us_legacy' as reference_population_key,
  construct_type,
  case
    when industry is null and level is not null then 'level'
    when level is null and industry is not null then 'industry'
    when industry is not null and level is not null then 'industry_level'
  else null end as benchmark_population_type,
  whole_person_model_version,
  industry,
  level,
  key,
  mean
FROM unioned
