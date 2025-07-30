WITH construct_benchmark_scores AS (
  SELECT * FROM {{ ref('stg_app__construct_benchmark_scores') }}
),

constructs AS (
  SELECT * FROM {{ ref('stg_assessment__constructs') }}
),

final as (

    SELECT
      construct_key,
      whole_person_model_version,
      industry,
      employee_level,
      scale_score_mean
    FROM construct_benchmark_scores
    WHERE construct_key IN (
      SELECT construct_key FROM constructs
    )

    UNION ALL

    SELECT
      derived_key AS construct_key,
      whole_person_model_version,
      industry,
      employee_level,
      scale_score_mean
    FROM construct_benchmark_scores
    WHERE derived_key IN (
      SELECT construct_key FROM constructs
    )

)

select
  {{ dbt_utils.surrogate_key(['construct_key', 'industry', 'employee_level']) }} AS primary_key,
  *
from final