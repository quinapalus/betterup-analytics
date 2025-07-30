with date_spine as (
-- from https://github.com/dbt-labs/dbt-utils#date_spine-source

    {{ dbt_utils.date_spine(
    datepart ="day",
    start_date = "to_date('2023-04-16', 'YYYY-MM-DD')",
    end_date="current_date+1") }}
),

base as (

    select * from {{ ref('int_coach__coach_profiles_snapshot') }}
    where is_last_snapshot_of_day

),

profile_island_attributes_snapshot as (

    select * from {{ ref('int_coach__profile_island_attributes_snapshot') }}
    where is_last_snapshot_of_day

),

coach_profiles_daily_snapshot as (

    select
        {{ dbt_utils.surrogate_key(['cp.coach_profile_uuid', 'date_day']) }}  as history_primary_key,
        date_trunc('day', date_day) as as_of_date,
        iff(date_trunc('day', date_day) = max(date_trunc('day', date_day)) over() and cp.is_current_version = true, true, false) as is_currently_valid,

        -- coach profile ids
        cp.coach_profile_uuid,
        cp.coach_profile_id, -- still functional but only for US data (keeping for versions item_id join)

        -- user fields
        cp.coach_id,
        cp.user_uuid,

        --coach profile timestamps
        cp.updated_at,
        cp.created_at,

        -- coach profile boolean coach types
        cp.is_primary_coach,
        cp.is_consumer,
        cp.is_on_demand_coach,
        cp.is_qa_coach,
        cp.is_care_coach,
        cp.is_peer_coach,
        cp.is_group_coach,

        -- island attributes to coalesce
        coalesce(pia.max_member_count, cp.max_member_count) as max_member_count,

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


        -- snapshot fields
        cp.valid_from,
        cp.valid_to

    from base cp
    inner join date_spine
      on cp.valid_from::date <= date_spine.date_day
      and (cp.valid_to::date > date_spine.date_day or cp.valid_to is null)
    left join profile_island_attributes_snapshot pia
        on cp.coach_profile_uuid = pia.coach_profile_uuid
        and pia.valid_from::date <= date_spine.date_day
        and (pia.valid_to::date > date_spine.date_day or pia.valid_to is null)

)

select * from coach_profiles_daily_snapshot