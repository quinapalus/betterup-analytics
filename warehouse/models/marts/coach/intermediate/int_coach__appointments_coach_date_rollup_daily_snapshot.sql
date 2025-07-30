with date_spine as (
-- from https://github.com/dbt-labs/dbt-utils#date_spine-source

    {{ dbt_utils.date_spine(
    datepart ="day",
    start_date = "to_date('2023-04-16', 'YYYY-MM-DD')",
    end_date="current_date+1") }}
),

base as (

    select * from {{ ref('int_app__appointments_snapshot') }}
    where is_last_snapshot_of_day

),

coach_date_rollup as (

    select
      coach_id,
      date_trunc('day', starts_at) as appointment_starts_at_date,
      date_trunc('day', date_day) as as_of_date,

      -- sum daily appointment bookings
      sum(appointment_length) as total_appointment_minutes,
      sum(appointment_length_hours) as total_appointment_hours

    from base
    inner join date_spine
      on base.valid_from::date <= date_spine.date_day
      and (base.valid_to::date > date_spine.date_day or base.valid_to is null)
    where canceled_at is null
    group by 1,2,3
)

select
    {{ dbt_utils.surrogate_key(['coach_id', 'appointment_starts_at_date', 'as_of_date']) }} AS history_primary_key,
    *
from coach_date_rollup