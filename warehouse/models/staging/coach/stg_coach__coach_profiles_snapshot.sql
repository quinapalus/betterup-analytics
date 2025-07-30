{{
  config(
    tags=["eu"]
  )
}}

with snapshot_coach_profiles as (

    select * from {{ ref('snapshot_coach__coach_profiles') }}

),

renamed AS (
-- following are the subset of coach profile fields needed for snapshot

    select
    -- ids
    {{ dbt_utils.surrogate_key(['id', 'uuid', 'dbt_valid_to', 'dbt_valid_from']) }} as history_primary_key,
    id as coach_profile_id, --still functional but only for us data
    uuid as coach_profile_uuid,

    -- coach profile timestamps
    created_at,
    updated_at,

    -- coach profile boolean coach types
    primary as is_primary_coach,
    consumer as is_consumer,
    coalesce(on_demand, false) as is_on_demand_coach,
    qa as is_qa_coach,
    care as is_care_coach,
    peer as is_peer_coach,
    "{{ environment_reserved_word_column('group') }}" as is_group_coach,

    -- fields which will be replaced w Profile Island Attributes
    max_member_count,

    --misc coach profile fields wanted in snapshot
    hiring_tier,
    segment_priority_level,
    languages as staffing_languages,
    tier as staffing_tier,
    coaching_cloud,
    coaching_varieties,
    staffable_state,
    eea_eligible as is_eea_eligible,
    fed_eligible as is_fed_eligible,
    preferred_weekly_hours,
    preferred_weekly_hours_updated_at,

    -- snapshot fields
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to

    from snapshot_coach_profiles

)

select * from renamed
