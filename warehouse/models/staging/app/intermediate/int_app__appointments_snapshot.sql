with appointment_snapshot as (

    select * from {{ ref('stg_app__appointments_snapshot') }}

),

deleted_records AS (
    select
      item_id
    from {{ ref('stg_app__versions_delete') }}
    where item_type = 'Appointment'
),

final as (

    select
        a.*,

        --derived appointment fields
        round(appointment_length/60, 2) as appointment_length_hours,

        --derived snapshot fields
        a.valid_to is null as is_current_version,
        row_number() over(
          partition by a.appointment_id
          order by a.valid_from
        ) as version,
        case when
          row_number() over(
            partition by a.appointment_id, date_trunc('day', a.valid_from)
            order by a.valid_from desc
          ) = 1 then true else false end as is_last_snapshot_of_day
    from appointment_snapshot a
    left join deleted_records dr ON a.appointment_id = dr.item_id
    where dr.item_id is null

)

select * from final