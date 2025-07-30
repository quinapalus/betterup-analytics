WITH construct_distributions AS (

  SELECT * FROM {{ ref('dbt__construct_distributions') }}

),

organizations AS (

  SELECT * FROM {{ ref('stg_app__organizations') }}

),

renamed as (
SELECT
  o.organization_id,
  cd.construct_reference_population_id,
  cd.construct_reference_population_uuid,
  cd.construct_id,
  cd.construct_key,
  cd.reference_population_id,
  cd.reference_population_key,
  cd.reference_population_subgroup_id,
  cd.reference_population_subgroup_key,
  cd.score_mean,
  cd.score_standard_deviation,
  cd.scale_min_score,
  cd.scale_max_score,
  cd.scale_mean,
  cd.scale_standard_deviation
FROM organizations AS o
INNER JOIN construct_distributions AS cd
  ON o.reference_population_id = cd.reference_population_id
),

final as (
  select
    {{dbt_utils.surrogate_key(['organization_id', 'construct_reference_population_id', 'construct_id', 'reference_population_id']) }} as _unique,
    *
  from renamed
)

select * from final