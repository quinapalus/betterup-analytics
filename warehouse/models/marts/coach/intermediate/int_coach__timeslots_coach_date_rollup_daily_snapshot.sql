with date_spine as (
-- from https://github.com/dbt-labs/dbt-utils#date_spine-source

    {{ dbt_utils.date_spine(
    datepart ="day",
    start_date = "to_date('2023-04-16', 'YYYY-MM-DD')",
    end_date="current_date+1") }}
),

base as (

    select * from {{ ref('int_app__timeslots_snapshot') }}
    where is_last_snapshot_of_day

),

coach_date_rollup as (

    select
      coach_id,
      date_trunc('day', starts_at) as timeslot_date,
      date_trunc('day', date_day) as as_of_date,

      -- sum every timeslot minute/hours field
      sum(available_minutes) as total_available_minutes,
      sum(available_hours) as total_available_hours,

      sum(external_unavailable_minutes) as total_external_unavailable_minutes,
      sum(external_unavailable_hours) as total_external_unavailable_hours,

      sum(timeslot_minutes) as total_timeslot_minutes,
      sum(timeslot_hours) as total_timeslot_hours,

      sum(timeslot_minutes_minus_external) as total_timeslot_minutes_minus_external,
      sum(timeslot_hours_minus_external) as total_timeslot_hours_minus_external

    from base
    inner join date_spine
      on base.valid_from::date <= date_spine.date_day
      and (base.valid_to::date > date_spine.date_day or base.valid_to is null)
    group by 1,2,3
)

select
    *,
    {{ dbt_utils.surrogate_key(['coach_id', 'timeslot_date', 'as_of_date']) }} AS history_primary_key
from coach_date_rollup