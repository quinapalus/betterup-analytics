with coach_assignments_snapshot as (

    select * from {{ ref('snapshot_app__coach_assignments') }}

),

final as (

    select
        -- ids
        {{ dbt_utils.surrogate_key(['id','dbt_valid_from','dbt_valid_to']) }} as history_primary_key,
        id as coach_assignment_id,
        coach_id,
        coach_recommendation_id,
        last_appointment_id,
        next_appointment_id,
        specialist_vertical_uuid,
        specialist_vertical_id,
        user_id as member_id,
        group_coaching_registration_id,
        conversation_id,
        group_coaching_cohort_id,
        last_message_id,

        --dates
        created_at,
        ended_at,
        member_last_active_at,
        last_message_sent_at,
        next_appointment_at,

        --misc
        ended_reason,
        role,
        specialty,
        updated_at,
        user_unread_message_count,
        coaching_cloud,
        coach_unread_message_count,
        debrief360,

        --snapshot fields
        dbt_valid_from as valid_from,
        dbt_valid_to as valid_to
    from coach_assignments_snapshot

)

select * from final