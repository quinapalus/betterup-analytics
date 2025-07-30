{{
  config(
    tags=['classification.c3_confidential','eu']
  )
}}


SELECT
{{ dbt_utils.star(from=ref('fact_coach_network_daily_snapshot'), relation_alias='fcn') }},
{{ dbt_utils.star(from=ref('fact_coach_work_rate_daily_snapshot'), except=["APP_COACH_ID", "DATE_KEY"], relation_alias='fcw') }},
{{ dbt_utils.star(from=ref('dim_coach'), except=["COACH_KEY"], relation_alias='dc') }},
{{ dbt_utils.star(from=ref('dim_date'), except=["DATE_KEY"], relation_alias='dd') }},
{{ is_current_date('fcn.date_key') }} AS is_current_date
FROM {{ref('fact_coach_network_daily_snapshot')}} AS fcn
INNER JOIN {{ref('dim_coach')}} AS dc ON fcn.coach_key = dc.coach_key
INNER JOIN {{ref('dim_date')}} AS dd ON fcn.date_key = dd.date_key
LEFT OUTER JOIN {{ref('fact_coach_work_rate_daily_snapshot')}} AS fcw
  ON dc.coach_app_coach_id = fcw.app_coach_id AND
     fcn.date_key = fcw.date_key
