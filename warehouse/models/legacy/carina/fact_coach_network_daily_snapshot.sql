{{
  config(
    tags=['classification.c3_confidential'],
    materialized='incremental',
    unique_key='date_key',
    incremental_strategy='delete+insert'
  )
}}

WITH coach AS (

  SELECT * FROM {{ref('dbt_coach')}}

),

dim_date AS (

  SELECT * FROM {{ref('dim_date')}}

),

fiscal_quarter_arr_goal AS (

  SELECT
    date_key AS _date_key,
    CASE
      -- stub current quarter ARR goal
      WHEN fiscal_year_quarter = '2020-Q2' THEN 26898000::int
      ELSE NULL
    END AS fiscal_quarter_arr_goal
  FROM dim_date

),

joined as (
SELECT
  {{ date_key('current_date') }} AS date_key,
  c.coach_key,
  c.coach_state,
  c.is_in_network,
  c.pipeline_stage,
  c.days_since_application,
  c.days_since_hire,
  c.priority_language,
  COALESCE(INITCAP(c.staffing_tier), 'N/A') AS coach_staffing_tier,
  c.staffable_state,
  -- create boolean fields for selected staffing segments
  {% for segment in ['employee_ic', 'employee_manager', 'friends_family', 'trial_cognitive', 'trial_test'] %}
  -- use IS TRUE to map NULL input values for Pipeline coaches to false
    coalesce(
        array_contains(
            'segment_{{ segment }}'::variant, c.staffing_qualifications) = true,
            false)
                AS coach_is_staffable_segment_{{ segment }},
  {% endfor %}
  -- assume theoretical capacity of 30 members/coach
  30::int AS seats_theoretical_count,
  c.seats_desired_count,
  c.seats_occupied_count,
  c.seats_available_count,
  -- assume $3600 annual revenue/member for ARR calculations
  3600 AS arr_per_seat,
  30 * 3600 AS coach_arr_theoretical,
  c.seats_desired_count * 3600 AS coach_arr_desired,
  c.seats_occupied_count * 3600 AS coach_arr_actual,
  c.seats_available_count * 3600 AS coach_arr_available,
  arr_goal.fiscal_quarter_arr_goal
FROM coach AS c
INNER JOIN fiscal_quarter_arr_goal AS arr_goal
  ON {{ date_key('current_date') }} = arr_goal._date_key
WHERE c.is_in_network
),

final as (
  select
    {{dbt_utils.surrogate_key(['date_key', 'coach_key'])}} as _unique,
    *
  from joined
)

select * from final