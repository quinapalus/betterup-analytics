WITH subdimension_benchmark_scores_reference_population_2 AS (

  SELECT * FROM {{ref('int_app__subdimension_benchmark_scores_reference_population_2')}}

),

dimension_benchmark_scores_reference_population_2 AS (

  SELECT * FROM {{ref('int_app__dimension_benchmark_scores_reference_population_2')}}

),

derived_construct_benchmark_scores_reference_population_2 AS (

  SELECT * FROM {{ref('int_app__derived_construct_benchmark_scores_reference_population_2')}}

),

construct_distributions AS (
    -- bring in global reference population means and standard deviation to scale benchmarks below
    SELECT * from {{ref('dbt__construct_distributions')}}

),

global_construct_distributions_multiple_subgroups AS (
   SELECT
       construct_key,
       --list subgroups that exist for construct, used in next CTE to filter
       LISTAGG(DISTINCT reference_population_subgroup_key, ',') WITHIN GROUP (ORDER BY reference_population_subgroup_key) AS list_subgroups
   FROM construct_distributions
   WHERE reference_population_key = 'global_2022'
   GROUP BY 1
 ),

 global_construct_distributions AS (

    SELECT
        cd.*
    FROM construct_distributions AS cd
    LEFT JOIN global_construct_distributions_multiple_subgroups AS g ON cd.construct_key = g.construct_key
    WHERE cd.REFERENCE_POPULATION_KEY = 'global_2022'
       -- exclude 360/wpf values, because they copy ref pop 1 benchmark values and do not reflect ref pop 2
       AND (reference_population_subgroup_key IS NULL OR reference_population_subgroup_key NOT IN ('360', 'wpf'))
       -- if this combination of subgroups exists for construct, use manager value. manager and IC values are copies.
       AND IFF(list_subgroups = '360,individual_contributor,manager', reference_population_subgroup_key = 'manager', 1=1)

),

unioned AS (

  SELECT
    reference_population_id,
    reference_population_key,
    benchmark_population_type,
    'subdimension' as construct_type,
    country,
    industry,
    level,
    key,
    mean
  FROM subdimension_benchmark_scores_reference_population_2

  UNION ALL

  SELECT
    reference_population_id,
    reference_population_key,
    benchmark_population_type,
    'dimension' as construct_type,
    country,
    industry,
    level as level,
    dimension_key AS key,
    mean
  FROM dimension_benchmark_scores_reference_population_2

  UNION ALL

  SELECT
    reference_population_id,
    reference_population_key,
    benchmark_population_type,
    'derived_construct' as construct_type,
    country,
    industry,
    level,
    key,
    mean
  FROM derived_construct_benchmark_scores_reference_population_2

),

scaled as (

    SELECT
      {{ dbt_utils.surrogate_key(['u.key', 'u.country', 'u.industry', 'u.level']) }} AS primary_key,
      u.construct_type,
      u.reference_population_id,
      u.reference_population_key,
      u.benchmark_population_type,
      u.country,
      u.industry,
      u.level,
      u.key,
      -- Scale z_score to desired mean and standard deviation, bounded by min and max scores defined in the constructs table
      GREATEST(cd.scale_min_score,
        LEAST(cd.scale_max_score,
            ((u.mean - cd.score_mean) / cd.score_standard_deviation) * cd.scale_standard_deviation + cd.scale_mean
        )
      ) AS mean
    FROM unioned u
    INNER JOIN global_construct_distributions AS cd ON u.key = cd.construct_key

)

SELECT * FROM scaled