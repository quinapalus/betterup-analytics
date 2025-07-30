{{
  config(
    tags=["eu"]
  )
}}

with coach_profiles_snapshot as (

  select * from {{ ref('stg_coach__coach_profiles_snapshot') }}

),

users as (

  select * from {{ ref('stg_app__users') }}

),

specialist_verticals as (

  select * from {{ ref('stg_curriculum__specialist_verticals') }}

),

deleted_records AS (
    select
      item_id
    from {{ ref('stg_app__versions_delete') }}
    where item_type = 'CoachProfile'
),

-- The following 2 CTEs cleans the array field 'coaching_varieties' from coach_profiles,
 -- taking the field from a concatenated 'specialist_vertical_' + specialist_vertical_id (ex: 'specialist_vertical_9')
 -- to convert the descriptive key from specialist_verticals (ex: 'navigating_uncertainty')
 -- IF YOU MAKE ANY UPDATES TO THIS LOGIC, DO THE SAME IN INT_COACH__COACH_PROFILES

coaching_varieties_unnested as (

  select
    cp.coach_profile_uuid,
    coaching_varieties.value::string as coaching_varieties_unnested
  from coach_profiles_snapshot as cp,
    lateral flatten(input => cp.coaching_varieties) coaching_varieties

),

coaching_varieties_cleaned as (
  select
    cvu.coach_profile_uuid,
    array_agg(coalesce(sv.key, cvu.coaching_varieties_unnested)) as coaching_varieties
  from coaching_varieties_unnested cvu
  left join specialist_verticals sv
    on 'specialist_vertical_'||sv.specialist_vertical_id = cvu.coaching_varieties_unnested
  group by 1

),

final as (
-- following are the subset of coach profile fields needed for snapshot

    select
        -- coach profile IDs
        {{ dbt_utils.surrogate_key(['cp.coach_profile_id', 'cp.coach_profile_uuid', 'valid_to', 'valid_from']) }} as history_primary_key,
        cp.coach_profile_id, --still functional but only for us data
        cp.coach_profile_uuid,

        -- user fields
        u.user_id as coach_id,
        u.user_uuid,

        -- coach profile timestamps
        cp.created_at,
        cp.updated_at,

        -- coach profile boolean coach types
        cp.is_primary_coach,
        cp.is_consumer,
        is_on_demand_coach, -- rationalize NULL values to false
        cp.is_qa_coach,
        cp.is_care_coach,
        cp.is_peer_coach,
        cp.is_group_coach,

        --misc coach profile fields wanted in snapshot
        cp.hiring_tier,
        cp.segment_priority_level,
        cp.staffing_languages,
        cp.staffing_tier,
        cp.coaching_cloud,
        coaching_varieties_cleaned.coaching_varieties,
        cp.staffable_state,
        cp.is_eea_eligible,
        cp.is_fed_eligible,
        cp.preferred_weekly_hours,
        cp.preferred_weekly_hours_updated_at,


        -- fields which will be replaced w Profile Island Attributes
        cp.max_member_count,

        -- snapshot fields
        cp.valid_from,
        cp.valid_to,

        --derived snapshot fields
        cp.valid_to is null as is_current_version,
        row_number() over(
          partition by cp.coach_profile_uuid
          order by cp.valid_from
        ) as version,
        case when
          row_number() over(
            partition by cp.coach_profile_uuid, date_trunc('day', cp.valid_from)
            order by cp.valid_from desc
          ) = 1 then true else false end as is_last_snapshot_of_day

    from coach_profiles_snapshot as cp
    inner join users as u
        on cp.coach_profile_uuid = u.coach_profile_uuid
    left join deleted_records dr
        on cp.coach_profile_id = dr.item_id
    left join coaching_varieties_cleaned
        on cp.coach_profile_uuid = coaching_varieties_cleaned.coach_profile_uuid
    where dr.item_id is null
)

select * from final