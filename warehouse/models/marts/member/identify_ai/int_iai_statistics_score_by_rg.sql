{{ config(
    tags=["identify_ai_metrics"],
) }}

WITH normalized_score AS (

  SELECT reporting_group_id,
  member_id,
  score
  FROM {{ref('member_identify_ai_score_calculation')}}

)

--by rg
SELECT
    reporting_group_id,
    avg(score) AS avg_score,
    median(score) AS median_score,
    mode(score) AS mode_score
FROM normalized_score
GROUP BY reporting_group_id
