with timeslots_snapshot as (

    select * from {{ ref('stg_app__timeslots_snapshot') }}

),

deleted_records AS (
    SELECT
      item_id
    FROM {{ ref('stg_app__versions_delete') }}
    WHERE item_type = 'Timeslot'
),

final as (
    select
        --ids
        {{ dbt_utils.surrogate_key(['timeslot_id', 'valid_from', 'valid_to']) }} AS history_primary_key,
        timeslot_id,
        coach_id,

        --dates
        starts_at,
        ends_at,
        created_at,
        updated_at,

        --snapshot fields
        valid_from,
        valid_to,

        --duration fields, including new derivations
        available_minutes,
        round(available_minutes/60, 2) as available_hours,

        external_unavailable_minutes,
        round(external_unavailable_minutes/60, 2) as external_unavailable_hours,

        timestampdiff('minute', starts_at, ends_at) as timeslot_minutes,
        round((timestampdiff('minute', starts_at, ends_at))/60,2) as timeslot_hours,

        timeslot_minutes - coalesce(external_unavailable_minutes,0) as timeslot_minutes_minus_external,
        timeslot_hours - coalesce(external_unavailable_hours,0) as timeslot_hours_minus_external,

        --derived snapshot fields
        t.valid_to is null as is_current_version,
        row_number() over(
          partition by t.timeslot_id
          order by t.valid_from
        ) as version,
        case when
          row_number() over(
            partition by t.timeslot_id, date_trunc('day', t.valid_from)
            order by t.valid_from desc
          ) = 1 then true else false end as is_last_snapshot_of_day
    from timeslots_snapshot t
    left join deleted_records dr ON t.timeslot_id = dr.item_id
    where dr.item_id is null
)

select * from final