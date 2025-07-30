{{ config(
    tags=["identify_ai_metrics"],
) }}

WITH normalized_score AS (

  SELECT reporting_group_id,
  member_id,
  score
  FROM {{ref('member_identify_ai_score_calculation')}}

)

--global
, final as (

SELECT
    avg(score) AS avg_score,
    median(score) AS median_score,
    mode(score) AS mode_score
FROM normalized_score

)

select
  final.*,
  {{ dbt_utils.surrogate_key(['median_score', 'avg_score','mode_score']) }} AS primary_key
from final
