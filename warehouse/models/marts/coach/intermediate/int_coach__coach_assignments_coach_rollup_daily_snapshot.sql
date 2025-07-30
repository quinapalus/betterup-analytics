{% set roles = ["primary", "care", "secondary"] %}

with date_spine as (
-- from https://github.com/dbt-labs/dbt-utils#date_spine-source

    {{ dbt_utils.date_spine(
    datepart ="day",
    start_date = "to_date('2023-04-16', 'YYYY-MM-DD')",
    end_date="current_date+1") }}
),

base as (

    select * from {{ ref('int_app__coach_assignments_snapshot') }}
    where is_last_snapshot_of_day

),

track_assignments AS (

  SELECT * FROM {{ref('stg_app__track_assignments')}}

),

tracks AS (

  SELECT * FROM {{ref('dim_tracks')}}

),

coach_assignment_daily_snapshot as (

    select
      {{ dbt_utils.surrogate_key(['coach_assignment_id', 'date_day']) }}  as history_primary_key,
      date_trunc('day', date_day) as as_of_date,
      iff(date_trunc('day', date_day) = max(date_trunc('day', date_day)) over() and is_current_version = true, true, false) as is_currently_valid,
      base.*
    from base
    inner join date_spine
      on base.valid_from::date <= date_spine.date_day
      and (base.valid_to::date > date_spine.date_day or base.valid_to is null)
),

coach_assignment_daily_snapshot_rollup AS (

  SELECT
   {{ dbt_utils.surrogate_key(['coach_id', 'as_of_date']) }}  as history_primary_key,
    coach_id,
    as_of_date,
    -- count members that coach has for each role
    {% for role in roles %}
        count(distinct
            iff(role = '{{role}}', member_id, null)
        ) as {{role}}_seats_occupied,
    {% endfor %}
    -- count active members that coach has for each role
    {% for role in roles -%}
        count(distinct
            iff(role = '{{role}}' and member_last_active_at >= dateadd(day, -30, current_date), member_id, null)
        ) AS {{role}}_active_seats_occupied
        {%- if not loop.last -%},{%- endif %}
    {%- endfor %}

  FROM coach_assignment_daily_snapshot
  WHERE ended_at IS NULL
    AND member_id NOT IN (
      -- filter out any seats occupied by current QA members, i.e. Mock Coach members
      SELECT ta.member_id
      FROM track_assignments AS ta
      INNER JOIN tracks AS t ON ta.track_id = t.track_id
      WHERE ta.ended_at IS NULL
        AND t.deployment_type = 'qa'
    )
  GROUP BY 1,2,3

)

select * from coach_assignment_daily_snapshot_rollup