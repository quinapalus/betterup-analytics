{{
  config(
    tags=['classification.c3_confidential']
  )
}}

SELECT
{{ dbt_utils.star(from=ref('fact_member_satisfaction_response_agg_by_coach'), relation_alias='frc') }},
{{ dbt_utils.star(from=ref('dim_coach'), except=["COACH_KEY"], relation_alias='dc') }},
{{ dbt_utils.star(from=ref('dim_assessment_item'), except=["ASSESSMENT_ITEM_KEY"], relation_alias='di') }}
FROM {{ref('fact_member_satisfaction_response_agg_by_coach')}} AS frc
INNER JOIN {{ref('dim_coach')}} AS dc
  ON frc.coach_key = dc.coach_key
INNER JOIN {{ref('dim_assessment_item')}} AS di
  ON frc.assessment_item_key = di.assessment_item_key
