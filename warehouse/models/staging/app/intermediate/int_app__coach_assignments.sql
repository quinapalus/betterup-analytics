{{
  config(
    tags=["eu"]
  )
}}

with coach_assignments as (

    select * from {{ ref('stg_app__coach_assignments') }}

),

deleted_records AS (
    select
      item_id
    from {{ ref('stg_app__versions_delete') }}
    where item_type = 'CoachAssignment'
),

final as (

    select
        coach_assignment_id,
        coach_id,
        coach_recommendation_id,
        created_at,
        ended_at,
        ended_reason,
        last_appointment_id,
        next_appointment_id,
        role,
        specialist_vertical_uuid,
        specialist_vertical_id, --still functional but only for US data
        updated_at,
        member_id,
        user_unread_message_count,
        member_last_active_at,
        coach_unread_message_count,
        last_message_sent_at,
        last_message_id,
        next_appointment_at,
        group_coaching_registration_id,
        debrief360,

        -- derived fields
        ended_at is not null as coach_assignment_ended
    from coach_assignments c
    left join deleted_records dr
        on c.coach_assignment_id = dr.item_id
    where
        -- remove destroyed records
        dr.item_id is null
        -- filter out BetterUp support
        and coach_id NOT IN (389, 5427)
    -- order according to row number requirements, but add additional descending-based ordering
    -- to prioritize open coach assignment or the assignment with the most recent date.
    qualify row_number() over (
        partition by member_id, coach_id, date_trunc('day', created_at)
        order by member_id, coach_id, date_trunc('day', created_at) asc, coalesce(ended_at, current_timestamp) desc
    ) = 1
)

select * from final