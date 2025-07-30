with base as (

    select * from {{ ref('int_coach__timeslots_coach_date_rollup_daily_snapshot') }}

),

upcoming_hours as (

    select
      {{ dbt_utils.surrogate_key(['coach_id', 'as_of_date']) }} as history_primary_key,
      coach_id,
      as_of_date,
      -- hours and minutes next 14 days
      sum(total_timeslot_hours_minus_external) as upcoming_timeslot_hours_minus_external_14d,
      sum(total_timeslot_minutes_minus_external) as upcoming_timeslot_minutes_minus_external_14d,
      -- hours and minutes next 3 days
      sum(
        iff(
            timeslot_date <= dateadd('day', 3, as_of_date),
            total_timeslot_hours_minus_external,
            null
        )
      ) as upcoming_timeslot_hours_minus_external_3d,
      sum(
        iff(
           timeslot_date <= dateadd('day', 3, as_of_date),
           total_timeslot_minutes_minus_external,
           null
        )
      ) as upcoming_timeslot_minutes_minus_external_3d,
      -- hours and minutes next 7 days
      sum(
        iff(
            timeslot_date <= dateadd('day', 7, as_of_date),
            total_timeslot_hours_minus_external,
            null
        )
      ) as upcoming_timeslot_hours_minus_external_7d,
      sum(
        iff(
           timeslot_date <= dateadd('day', 7, as_of_date),
           total_timeslot_minutes_minus_external,
           null
        )
      ) as upcoming_timeslot_minutes_minus_external_7d
    from base
    -- coach rollup only on next 14 days
    where
        --"upcoming" timeslots from perspective of historic snapshot,
        -- so timeslot date greater than as_of_date (don't include "today"/as_of_date in forecast)
        timeslot_date > as_of_date
        -- and timeslot date less than or equal to next 14 days
        and timeslot_date <= dateadd('day', 14, as_of_date)
    group by 1,2,3
)

select * from upcoming_hours