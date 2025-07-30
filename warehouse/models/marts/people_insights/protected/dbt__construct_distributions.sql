WITH constructs AS (

  SELECT * FROM {{ ref('stg_assessment__constructs') }}

),

construct_reference_populations AS (

  SELECT * FROM {{ ref('stg_assessment__construct_reference_populations') }}

),

reference_populations AS (

  SELECT * FROM {{ ref('stg_assessment__reference_populations') }}

),

reference_population_subgroups AS (

  SELECT * FROM {{ ref('stg_assessment__reference_population_subgroups') }}

)


SELECT
  crp.construct_reference_population_id,
  crp.construct_reference_population_uuid,
  c.construct_id,
  c.construct_key AS construct_key,
  c.construct_name,
  c.label_i18n,
  rp.reference_population_id,
  rp.key AS reference_population_key,
  rps.reference_population_subgroup_id,
  rps.key AS reference_population_subgroup_key,
  crp.score_mean,
  crp.score_standard_deviation,
  c.scale_min_score,
  c.scale_max_score,
  c.scale_mean,
  c.scale_standard_deviation
FROM construct_reference_populations AS crp
INNER JOIN constructs AS c ON crp.construct_id = c.construct_id
INNER JOIN reference_populations AS rp ON rp.reference_population_id = crp.reference_population_id
LEFT OUTER JOIN reference_population_subgroups AS rps
  ON rps.reference_population_subgroup_id = crp.reference_population_subgroup_id
