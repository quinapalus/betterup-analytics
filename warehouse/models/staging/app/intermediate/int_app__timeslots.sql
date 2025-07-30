with timeslots as (

    select * from {{ ref('stg_app__timeslots') }}

),

deleted_records AS (
    SELECT
      item_id
    FROM {{ ref('stg_app__versions_delete') }}
    WHERE item_type = 'Timeslot'
),

final as (
   select
        timeslot_id,
        coach_id,
        created_at,
        updated_at,
        starts_at,
        ends_at,
        available_minutes,
        round(available_minutes/60, 2) as available_hours,
        external_unavailable_minutes,
        round(external_unavailable_minutes/60, 2) as external_unavailable_hours,

        --derived fields
        timestampdiff('minute', starts_at, ends_at) as timeslot_minutes,
        round((timestampdiff('minute', starts_at, ends_at))/60,2) as timeslot_hours,
        timeslot_minutes - coalesce(external_unavailable_minutes,0) as timeslot_minutes_minus_external,
        timeslot_hours - coalesce(external_unavailable_hours,0) as timeslot_hours_minus_external

    from timeslots t
    left join deleted_records dr ON t.timeslot_id = dr.item_id
    where dr.item_id is null
)

select * from final