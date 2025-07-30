with coach_profiles as (

    select * from {{ ref('int_coach__coach_profiles_daily_snapshot') }}

),

-- this is a temporary fix due to a snapshot error causing lost data.
-- this will be fixed after a training on 5/17.
temporary_fix_coach_profiles as (

    select * from {{ ref('int_coach__coach_profiles') }}

),

timeslots as (

    select * from {{ ref('int_coach__timeslots_coach_rollup_daily_snapshot') }}

),

coach_assignments as (

    select * from {{ ref('int_coach__coach_assignments_coach_rollup_daily_snapshot') }}

),

appointments as (

    select * from {{ ref('int_coach__appointments_coach_rollup_daily_snapshot') }}

),

final as (

    select distinct
        cp.history_primary_key,
        cp.coach_id,
        cp.user_uuid,
        cp.as_of_date,
        cp.is_currently_valid,

        -- coach profile ids
        cp.coach_profile_uuid,
        cp.coach_profile_id, -- still functional but only for US data (keeping for versions item_id join)

        --coach profile timestamps
        cp.updated_at,
        cp.created_at,

        -- coach profile boolean coach types
        coalesce(cp.is_primary_coach, tcp.is_primary_coach) as is_primary_coach,
        coalesce(cp.is_consumer, tcp.is_consumer) as is_consumer,
        coalesce(cp.is_on_demand_coach, tcp.is_on_demand_coach) as is_on_demand_coach,
        coalesce(cp.is_qa_coach, tcp.is_qa_coach) as is_qa_coach,
        coalesce(cp.is_care_coach, tcp.is_care_coach) as is_care_coach,
        coalesce(cp.is_peer_coach, tcp.is_peer_coach) as is_peer_coach,
        coalesce(cp.is_group_coach, tcp.is_group_coach) as is_group_coach,

        -- island attributes
        cp.max_member_count,

        --misc coach profile fields wanted in snapshot
        cp.hiring_tier,
        cp.segment_priority_level,
        cp.staffing_languages,
        cp.staffing_tier,
        cp.coaching_cloud,
        cp.coaching_varieties,
        cp.staffable_state,
        cp.is_eea_eligible,
        cp.is_fed_eligible,
        cp.preferred_weekly_hours,
        cp.preferred_weekly_hours_updated_at,


        -- coach assignment fields
        ifnull(ca.primary_seats_occupied,0) as primary_seats_occupied,
        ifnull(ca.primary_active_seats_occupied,0) as primary_active_seats_occupied,
        ifnull(ca.care_seats_occupied,0) as care_seats_occupied,
        ifnull(ca.care_active_seats_occupied,0) as care_active_seats_occupied,
        ifnull(ca.secondary_seats_occupied,0) as secondary_seats_occupied,
        ifnull(ca.secondary_active_seats_occupied,0) as secondary_active_seats_occupied,

        -- timeslot fields
        ifnull(upcoming_timeslot_hours_minus_external_3d,0) as upcoming_timeslot_hours_minus_external_3d,
        ifnull(upcoming_timeslot_minutes_minus_external_3d,0) as upcoming_timeslot_minutes_minus_external_3d,
        ifnull(upcoming_timeslot_hours_minus_external_7d,0) as upcoming_timeslot_hours_minus_external_7d,
        ifnull(upcoming_timeslot_minutes_minus_external_7d,0) as upcoming_timeslot_minutes_minus_external_7d,
        ifnull(upcoming_timeslot_hours_minus_external_14d,0) as upcoming_timeslot_hours_minus_external_14d,
        ifnull(upcoming_timeslot_minutes_minus_external_14d,0) as upcoming_timeslot_minutes_minus_external_14d,

        -- appointment fields
        ifnull(upcoming_booking_hours_3d,0) as upcoming_booking_hours_3d,
        ifnull(upcoming_booking_hours_7d,0) as upcoming_booking_hours_7d,
        ifnull(upcoming_booking_hours_14d,0) as upcoming_booking_hours_14d

    from coach_profiles cp
    left join temporary_fix_coach_profiles tcp
        on cp.coach_profile_uuid = tcp.coach_profile_uuid
    left join coach_assignments ca
        on cp.coach_id = ca.coach_id
        and cp.as_of_date = ca.as_of_date
    left join timeslots t
        on cp.coach_id = t.coach_id
        and cp.as_of_date = t.as_of_date
    left join appointments a
        on cp.coach_id = a.coach_id
        and cp.as_of_date = a.as_of_date
)

select * from final
