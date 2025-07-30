with appointment_snapshot as (

    select * from {{ ref('snapshot_app__appointments') }}

),

final as (
    select
        --ids
        {{ dbt_utils.surrogate_key(['id','dbt_valid_from','dbt_valid_to']) }} as history_primary_key,
        id as appointment_id,
        uuid,
        timeslot_id,
        coach_id,
        creator_id,
        user_id as member_id,
        call_id,
        participant_id,
        canceled_by_user_id,
        coach_assignment_id,
        track_assignment_id,
        nylas_event_id,
        development_topic_id,
        post_session_member_assessment_id,
        product_subscription_assignment_id,
        facilitator_assignment_id,
        last_rescheduled_by_user_id,

        --dates
        created_at as appointment_created_at,
        updated_at as appointment_updated_at,
        starts_at,
        ends_at,
        started_at,
        complete_at,
        canceled_at,
        confirmed_at,
        original_starts_at,

        --misc
        complete_at IS NOT NULL AS is_completed,
        contact_method,
        length AS appointment_length,
        missed AS is_appointment_missed,
        recurring AS is_appointment_recurring,
        requested_length,
        requested_weekly_interval,
        token,
        sequence_number,

        --snapshot fields
        dbt_valid_from as valid_from,
        dbt_valid_to as valid_to
    from appointment_snapshot
)

select * from final