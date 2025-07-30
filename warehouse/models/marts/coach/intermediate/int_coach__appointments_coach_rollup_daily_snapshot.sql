with base as (

    select * from {{ ref('int_coach__appointments_coach_date_rollup_daily_snapshot') }}

),

upcoming_bookings as (

    select
        {{ dbt_utils.surrogate_key(['coach_id', 'as_of_date']) }} as history_primary_key,
        coach_id,
        as_of_date,
        sum(total_appointment_hours) as upcoming_booking_hours_14d,
        sum(
            iff(
                appointment_starts_at_date <= dateadd('day', 3, as_of_date),
                total_appointment_hours,
                null
            )
        ) as upcoming_booking_hours_3d,
        sum(
            iff(
                appointment_starts_at_date <= dateadd('day', 7, as_of_date),
                total_appointment_hours,
                null
            )
        ) as upcoming_booking_hours_7d
    from base
    -- coach rollup only on next 14 days
    where
        --"upcoming" appointments from perspective of historic snapshot,
        -- so appointment date greater than as_of_date (don't include "today"/as_of_date in forecast)
        appointment_starts_at_date > as_of_date
        -- and appointment date less than or equal to next 14 days
        and appointment_starts_at_date <= dateadd('day', 14, as_of_date)
    group by 1,2,3
)

select * from upcoming_bookings