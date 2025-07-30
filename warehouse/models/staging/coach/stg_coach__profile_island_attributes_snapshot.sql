with source as (

    select * from {{ ref('snapshot_coach__profile_island_attributes') }}

),

final as (
-- following are the subset of profile island attribute fields needed for snapshot

    select
        --ids
        {{ dbt_utils.surrogate_key(['uuid', 'dbt_valid_from', 'dbt_valid_to']) }} as history_primary_key,
        id AS profile_island_attribute_id,
        uuid AS profile_island_attribute_uuid,
        coach_profile_uuid,

        --other
        island,
        max_member_count,

        --dates
        created_at,
        updated_at,

        -- snapshot fields
        dbt_valid_from as valid_from,
        dbt_valid_to as valid_to

    from source

)

select * from final
