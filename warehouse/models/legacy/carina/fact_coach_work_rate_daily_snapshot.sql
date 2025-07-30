{%- set t30d_window -%}
  PARTITION BY cd.coach_id
  ORDER BY {{ date_key('cd.day') }}
  ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
{%- endset -%}

{%- set t60d_window -%}
  PARTITION BY cd.coach_id
  ORDER BY {{ date_key('cd.day') }}
  ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
{%- endset -%}

{%- set t90d_window -%}
  PARTITION BY cd.coach_id
  ORDER BY {{ date_key('cd.day') }}
  ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
{%- endset -%}

{% set cumulative_to_date_window %}
  PARTITION BY cd.coach_id
  ORDER BY {{ date_key('cd.day') }}
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
{%- endset -%}

WITH coach_date AS (

  SELECT * FROM {{ ref('dbt_coach_date') }}

),

coach_date_sessions AS (

  SELECT * FROM {{ ref('dbt_coach_date_sessions') }}

),

final as (
SELECT
  cd.coach_id AS app_coach_id,
  {{ date_key('cd.day') }} AS date_key,

  COALESCE(s.billable_session_count, 0) AS billable_session_count,
  COALESCE(s.billable_session_hours, 0) AS billable_session_hours,
  COALESCE(s.completed_session_count, 0) AS completed_session_count,
  COALESCE(s.completed_session_hours, 0) AS completed_session_hours,

  -- Calculate trailing 30 day aggregates:
  SUM(COALESCE(s.billable_session_count, 0)) OVER ({{t30d_window}})
    AS billable_session_count_t30d,
  SUM(COALESCE(s.billable_session_hours, 0)) OVER ({{t30d_window}})
    AS billable_session_hours_t30d,
  SUM(COALESCE(s.completed_session_count, 0)) OVER ({{t30d_window}})
    AS completed_session_count_t30d,
  SUM(COALESCE(s.completed_session_hours, 0)) OVER ({{t30d_window}})
    AS completed_session_hours_t30d,

  -- Calculate trailing 60 day aggregates:
  SUM(COALESCE(s.billable_session_count, 0)) OVER ({{t60d_window}})
    AS billable_session_count_t60d,
  SUM(COALESCE(s.billable_session_hours, 0)) OVER ({{t60d_window}})
    AS billable_session_hours_t60d,
  SUM(COALESCE(s.completed_session_count, 0)) OVER ({{t60d_window}})
    AS completed_session_count_t60d,
  SUM(COALESCE(s.completed_session_hours, 0)) OVER ({{t60d_window}})
    AS completed_session_hours_t60d,

  -- Calculate trailing 90 day aggregates:
  SUM(COALESCE(s.billable_session_count, 0)) OVER ({{t90d_window}})
    AS billable_session_count_t90d,
  SUM(COALESCE(s.billable_session_hours, 0)) OVER ({{t90d_window}})
    AS billable_session_hours_t90d,
  SUM(COALESCE(s.completed_session_count, 0)) OVER ({{t90d_window}})
    AS completed_session_count_t90d,
  SUM(COALESCE(s.completed_session_hours, 0)) OVER ({{t90d_window}})
    AS completed_session_hours_t90d,

  -- Calculate cumulative coach to date aggregates:
  SUM(COALESCE(s.billable_session_count, 0)) OVER ({{cumulative_to_date_window}})
    AS billable_session_count_coach_to_date,
  SUM(COALESCE(s.billable_session_hours, 0)) OVER ({{cumulative_to_date_window}})
    AS billable_session_hours_coach_to_date,
  SUM(COALESCE(s.completed_session_count, 0)) OVER ({{cumulative_to_date_window}})
    AS completed_session_count_coach_to_date,
  SUM(COALESCE(s.completed_session_hours, 0)) OVER ({{cumulative_to_date_window}})
    AS completed_session_hours_coach_to_date

FROM coach_date AS cd
LEFT OUTER JOIN coach_date_sessions AS s
  ON cd.coach_id = s.app_coach_id AND {{ date_key('cd.day') }} = s.date_key
)

select
    {{ dbt_utils.surrogate_key(['app_coach_id', 'date_key' ]) }} as primary_key,
    *
from final