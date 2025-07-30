{{
  config(
    tags=["eu"]
  )
}}

{% set construct_attributes_dict = {
    "model":"construct_model",
    "category":"wpm_category",
    "domain":"wpm_domain",
    "dimension":"wpm_dimension"} -%}

WITH construct_scores AS (

  SELECT * FROM {{ref('fact_construct_scores')}}

),

reporting_group_assignments AS (

    SELECT * FROM {{ref('dim_reporting_group_assignments')}}
)

SELECT
  {{ dbt_utils.surrogate_key(['s.primary_key', 'rga.primary_key']) }} AS primary_key,
  rga.reporting_group_id,
  {% for key, column_name in construct_attributes_dict.items() %}
  construct_attributes:{{ key }}::varchar as {{ column_name }},
  {% endfor %}
  {{ dbt_utils.star(from=ref('fact_construct_scores'), except=["PRIMARY_KEY"], relation_alias='s') }}
FROM construct_scores AS s
INNER JOIN reporting_group_assignments AS rga
  ON s.member_id = rga.member_id AND
     s.submitted_at >= rga.starts_at AND
     (rga.ended_at IS NULL OR s.submitted_at < rga.ended_at)
ORDER BY rga.reporting_group_id
