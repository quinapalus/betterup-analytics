with coach_assignments_snapshot as (

    select * from {{ ref('stg_app__coach_assignments_snapshot') }}

),

deleted_records AS (
    select
      item_id
    from {{ ref('stg_app__versions_delete') }}
    where item_type = 'CoachAssignment'
),

final as (

    select
        c.*,

        --derived snapshot fields
        c.valid_to is null as is_current_version,
        row_number() over(
          partition by c.coach_assignment_id
          order by c.valid_from
        ) as version,
        case when
          row_number() over(
            partition by c.coach_assignment_id, date_trunc('day', c.valid_from)
            order by c.valid_from desc
          ) = 1 then true else false end as is_last_snapshot_of_day
    from coach_assignments_snapshot c
    left join deleted_records dr ON c.coach_assignment_id = dr.item_id
    where dr.item_id is null
)

select * from final