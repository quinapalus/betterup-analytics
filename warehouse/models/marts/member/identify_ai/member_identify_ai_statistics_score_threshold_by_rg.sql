{{ config(
    tags=["identify_ai_metrics"],
) }}

WITH stats AS (

  SELECT * FROM {{ref('int_iai_statistics_score_by_rg')}}

)

, final as (

SELECT
median(median_score) AS median_score,
avg(median_score) AS avg_score
FROM stats

)

select
  final.*,
  {{ dbt_utils.surrogate_key(['median_score', 'avg_score']) }} AS primary_key
from final
