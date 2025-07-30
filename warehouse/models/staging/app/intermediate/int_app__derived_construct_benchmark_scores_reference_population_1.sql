with construct_benchmark_scores_reference_population_1 as (

    select * from {{ ref('stg_app__construct_benchmark_scores_reference_population_1') }}

),

construct_key_mapping as (

    select * from {{ ref('bu_construct_key_mapping') }}

),

constructs AS (

  SELECT * FROM {{ ref('stg_assessment__constructs') }}

),

benchmarks_mapping_joined as (
    SELECT
      b.*,
      m.derived_key
    from construct_benchmark_scores_reference_population_1 b
    left outer join construct_key_mapping m
        on b.key = m.construct_key
),

unioned as (
    -- Duplicate benchmark records per construct_key + derived_key combination
    SELECT
      key,
      whole_person_model_version,
      industry,
      level,
      mean
    FROM benchmarks_mapping_joined
    WHERE key IN (
      SELECT construct_key FROM constructs
    )

    UNION ALL

    SELECT
      derived_key AS key,
      whole_person_model_version,
      industry,
      level,
      mean
    FROM benchmarks_mapping_joined
    WHERE derived_key IN (
      SELECT construct_key FROM constructs
    )
),

final as (
  select
    --primary key
    {{ dbt_utils.surrogate_key(['key', 'whole_person_model_version', 'industry', 'level']) }} as _unique,
    *
  from unioned
)

SELECT * from final